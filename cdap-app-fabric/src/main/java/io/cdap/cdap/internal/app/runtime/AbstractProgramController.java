/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.runtime;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base implementation of {@link ProgramController} that governs state transitions as well
 * as {@link Listener} invocation mechanism.
 */
public abstract class AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramController.class);

  private final AtomicReference<State> state;
  private final ProgramRunId programRunId;
  private final Map<ListenerCaller, Cancellable> listeners;
  private final Listener caller;
  private final ExecutorService executor;

  private Throwable failureCause;
  private volatile long gracefulTimeoutMillis;

  protected AbstractProgramController(ProgramRunId programRunId) {
    this.state = new AtomicReference<>(State.STARTING);
    this.programRunId = programRunId;
    this.listeners = new HashMap<>();
    this.caller = new MultiListenerCaller();
    this.gracefulTimeoutMillis = -1L;

    // Create a single thread executor that doesn't keep core thread and the thread will shutdown when there
    // is no pending task. In this way, we don't need to shutdown the executor since there will be no thread
    // hanging around when it is idle.
    String name = programRunId.getParent() + "-" + programRunId.getRun();
    this.executor = new ThreadPoolExecutor(0, 1, 0, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder()
            .setNameFormat("pcontroller-" + name + "-%d").build());
  }

  @Override
  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  @Override
  public RunId getRunId() {
    return RunIds.fromString(programRunId.getRun());
  }

  @Override
  public final ListenableFuture<ProgramController> suspend() {
    LOG.trace("Suspend program {}", programRunId);
    if (!state.compareAndSet(State.ALIVE, State.SUSPENDING)) {
      return Futures.immediateFailedFuture(
          new IllegalStateException(
              "Suspension not allowed for " + programRunId + " in " + state.get()));
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor.execute(() -> {
      try {
        caller.suspending();
        doSuspend();
        state.set(State.SUSPENDED);
        result.set(AbstractProgramController.this);
        caller.suspended();
      } catch (Throwable t) {
        error(t, result);
      }
    });

    return result;
  }

  @Override
  public final ListenableFuture<ProgramController> resume() {
    LOG.trace("Resume program {}", programRunId);
    if (!state.compareAndSet(State.SUSPENDED, State.RESUMING)) {
      return Futures.immediateFailedFuture(
          new IllegalStateException(
              "Resumption not allowed for " + programRunId + " in " + state.get()));
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor.execute(() -> {
      try {
        caller.resuming();
        doResume();
        state.set(State.ALIVE);
        result.set(AbstractProgramController.this);
        caller.alive();
      } catch (Throwable t) {
        error(t, result);
      }
    });
    return result;
  }

  @Override
  public final ListenableFuture<ProgramController> stop() {
    LOG.trace("Stop program {}", programRunId);
    if (!state.compareAndSet(State.STARTING, State.STOPPING)
        && !state.compareAndSet(State.ALIVE, State.STOPPING)
        && !state.compareAndSet(State.SUSPENDED, State.STOPPING)) {
      return Futures.immediateFailedFuture(
          new IllegalStateException(
              "Stopping not allowed for " + programRunId + " in " + state.get()));
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor.execute(() -> {
      try {
        caller.stopping();
        doStop();
        state.set(State.KILLED);
        result.set(AbstractProgramController.this);
        caller.killed();
      } catch (Throwable t) {
        error(t, result);
      }
    });
    return result;
  }

  @Override
  public final ListenableFuture<ProgramController> stop(long timeout, TimeUnit timeoutUnit) {
    if (timeout < 0) {
      throw new IllegalStateException("Stop timeout must be >= 0");
    }

    LOG.trace("Stop program {} with timeout {}{}", programRunId, timeout, timeoutUnit);
    this.gracefulTimeoutMillis = timeoutUnit.toMillis(timeout);
    return stop();
  }

  @Override
  public void kill() {
    stop();
  }

  /**
   * Returns the graceful stop timeout in milliseconds. If it returns a value that is smaller than
   * {@code 0}, if means graceful stop timeout is not defined. It is up to the implementation of the
   * controller to determine stop timeout.
   */
  protected final long getGracefulTimeoutMillis() {
    return gracefulTimeoutMillis;
  }

  /**
   * Children call this method to signal the program is completed.
   */
  protected void complete() {
    complete(State.COMPLETED);
  }

  protected void complete(final State completionState) {
    LOG.trace("Program {} completed with state {}", programRunId, completionState);
    if (!state.compareAndSet(State.STARTING, completionState)
        && !state.compareAndSet(State.ALIVE, completionState)
        && !state.compareAndSet(State.SUSPENDED, completionState)) {
      LOG.warn("Cannot transit to COMPLETED state from {} state: {}", state.get(), programRunId);
      return;
    }
    executor.execute(() -> {
      state.set(completionState);
      if (State.KILLED.equals(completionState)) {
        caller.killed();
      } else if (State.ERROR.equals(completionState)) {
        // mark program as error when its in error state.
        caller.error(
            new Exception(String.format("Program %s completed with exception.", programRunId)));
      } else {
        caller.completed();
      }
    });
  }

  @Override
  public final Cancellable addListener(Listener listener, final Executor listenerExecutor) {
    Preconditions.checkNotNull(listener, "Listener shouldn't be null.");
    Preconditions.checkNotNull(listenerExecutor, "Executor shouldn't be null.");

    final ListenerCaller caller = new ListenerCaller(listener, listenerExecutor);
    final Cancellable cancellable = () -> {
      // Simply remove the listener from the map through the executor and block on the completion
      Futures.getUnchecked(executor.submit(() -> {
        listeners.remove(caller);
      }));
    };

    try {
      // Use a blocking queue to communicate the Cancellable to return
      BlockingQueue<Cancellable> result = new ArrayBlockingQueue<>(1);

      // Use the single thread executor to add the listener and call init
      executor.submit(() -> {
        Cancellable existing = listeners.get(caller);
        if (existing == null) {
          listeners.put(caller, cancellable);
          result.put(cancellable);
          caller.init(getState(), getFailureCause());
        } else {
          result.put(existing);
        }
        return null;
      });
      return result.take();
    } catch (Exception e) {
      // Not expecting exception since the Callable only do action on Map and calling caller.init, which
      // already have exceptions handled inside the method. Also, we never shutdown the executor explicitly,
      // there shouldn't be interrupted exception as well.
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  @Override
  public final ListenableFuture<ProgramController> command(final String name, final Object value) {
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor.execute(() -> {
      try {
        doCommand(name, value);
        result.set(AbstractProgramController.this);
      } catch (Throwable t) {
        error(t, result);
      }
    });
    return result;
  }

  @Override
  public final State getState() {
    return state.get();
  }

  @Override
  public final Throwable getFailureCause() {
    return failureCause;
  }

  /**
   * Force this controller into error state.
   *
   * @param t The failure cause
   */
  protected final void error(final Throwable t) {
    executor.execute(() -> error(t, null));
  }

  /**
   * Children call this method to signal the program is started.
   */
  protected final void started() {
    LOG.trace("Program {} started", programRunId);
    if (!state.compareAndSet(State.STARTING, State.ALIVE)) {
      LOG.debug("Cannot transit to ALIVE state from {} state: {}", state.get(), programRunId);
      return;
    }
    executor.execute(() -> {
      state.set(State.ALIVE);
      caller.alive();
    });
  }

  protected abstract void doSuspend() throws Exception;

  protected abstract void doResume() throws Exception;

  protected abstract void doStop() throws Exception;

  protected abstract void doCommand(String name, Object value) throws Exception;

  /**
   * Force this controller into error state and set the failure into the given future. This method
   * should only be called from the single thread executor of this class.
   *
   * @param t The failure cause
   */
  private <V> void error(Throwable t, SettableFuture<V> future) {
    LOG.trace("Program {} forced to error", programRunId, t);
    failureCause = t;
    state.set(State.ERROR);
    if (future != null) {
      future.setException(t);
    }
    caller.error(t);
  }

  /**
   * Class for making calls to multiple {@link Listener}s on state change.
   */
  private final class MultiListenerCaller implements Listener {

    @Override
    public void init(State currentState, @Nullable Throwable cause) {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.init(currentState, cause);
      }
    }

    @Override
    public void suspending() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.suspending();
      }
    }

    @Override
    public void suspended() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.suspended();
      }
    }

    @Override
    public void resuming() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.resuming();
      }
    }

    @Override
    public void alive() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.alive();
      }
    }

    @Override
    public void stopping() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.stopping();
      }
    }

    @Override
    public void completed() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.completed();
      }
    }

    @Override
    public void killed() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.killed();
      }
    }

    @Override
    public void error(Throwable cause) {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.error(cause);
      }
    }
  }

  /**
   * Wrapper for making calls to {@link Listener} through an {@link Executor}.
   */
  private static final class ListenerCaller implements Listener {

    private final Listener listener;
    private final Executor executor;

    private ListenerCaller(Listener listener, Executor executor) {
      this.listener = listener;
      this.executor = executor;
    }

    private void execute(Runnable runnable, String methodName) {
      try {
        executor.execute(runnable);
      } catch (Throwable t) {
        String msg = String.format(
            "Exception while executing method '%s' on listener %s with executor %s.",
            methodName, listener, executor);
        LOG.error(msg, t);
      }
    }

    @Override
    public void init(final State currentState, @Nullable final Throwable cause) {
      execute(() -> listener.init(currentState, cause), "init");
    }

    @Override
    public void suspending() {
      execute(listener::suspending, "suspending");
    }

    @Override
    public void suspended() {
      execute(listener::suspended, "suspended");
    }

    @Override
    public void resuming() {
      execute(listener::resuming, "resuming");
    }

    @Override
    public void alive() {
      execute(listener::alive, "alive");
    }

    @Override
    public void stopping() {
      execute(listener::stopping, "stopping");
    }

    @Override
    public void completed() {
      execute(listener::completed, "completed");
    }

    @Override
    public void killed() {
      execute(listener::killed, "killed");
    }

    @Override
    public void error(final Throwable cause) {
      execute(() -> listener.error(cause), "error");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      // Only compare with the listener
      ListenerCaller other = (ListenerCaller) o;
      return Objects.equal(listener, other.listener);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(listener);
    }
  }
}
