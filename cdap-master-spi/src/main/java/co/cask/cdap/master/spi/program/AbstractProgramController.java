/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.master.spi.program;

import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Abstract base implementation of {@link ProgramController} that governs state transitions as well as
 * {@link Listener} invocation mechanism.
 */
public abstract class AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramController.class);

  private final AtomicReference<State> state;
  private final ProgramId programId;
  private final ProgramRunId programRunId;
  private final RunId runId;
  private final String componentName;
  private final Map<ListenerCaller, Cancellable> listeners;
  private final Listener caller;
  private final ExecutorService executor;
  private final String name;

  private Throwable failureCause;

  protected AbstractProgramController(ProgramRunId programRunId) {
    this(programRunId, null);
  }

  protected AbstractProgramController(ProgramRunId programRunId, @Nullable String componentName) {
    this.state = new AtomicReference<>(State.STARTING);
    this.programRunId = programRunId;
    this.programId = programRunId.getParent();
    this.runId = org.apache.twill.internal.RunIds.fromString(programRunId.getRun());
    this.componentName = componentName;
    this.listeners = new HashMap<>();
    this.caller = new MultiListenerCaller();
    this.name = programId + (componentName == null ? "" : "-" + componentName) + "-" + runId.getId();

    // Create a single thread executor that doesn't keep core thread and the thread will shutdown when there
    // is no pending task. In this way, we don't need to shutdown the executor since there will be no thread
    // hanging around when it is idle.
    this.executor = new ThreadPoolExecutor(0, 1, 0, TimeUnit.SECONDS,
                                           new LinkedBlockingQueue<>(),
                                           r -> new Thread(null, r, "pcontroller-" + name));
  }

  @Override
  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Nullable
  @Override
  public String getComponentName() {
    return componentName;
  }

  @Override
  public final Future<ProgramController> suspend() {
    LOG.trace("Suspend program {}", programId);
    CompletableFuture<ProgramController> future = new CompletableFuture<>();
    if (!state.compareAndSet(State.ALIVE, State.SUSPENDING)) {
      future.completeExceptionally(
        new IllegalStateException("Suspension not allowed for " + programId + " in " + state.get()));
      return future;
    }
    executor.execute(() -> {
      try {
        caller.suspending();
        doSuspend();
        state.set(State.SUSPENDED);
        future.complete(AbstractProgramController.this);
        caller.suspended();
      } catch (Throwable t) {
        error(t, future);
      }
    });

    return future;
  }

  @Override
  public final Future<ProgramController> resume() {
    LOG.trace("Resume program {}", programId);
    CompletableFuture<ProgramController> future = new CompletableFuture<>();
    if (!state.compareAndSet(State.SUSPENDED, State.RESUMING)) {
      future.completeExceptionally(
        new IllegalStateException("Resumption not allowed for " + name + " in " + state.get()));
      return future;
    }
    executor.execute(() -> {
      try {
        caller.resuming();
        doResume();
        state.set(State.ALIVE);
        future.complete(AbstractProgramController.this);
        caller.alive();
      } catch (Throwable t) {
        error(t, future);
      }
    });
    return future;
  }

  @Override
  public final Future<ProgramController> stop() {
    LOG.trace("Stop program {}", programId);
    if (!state.compareAndSet(State.STARTING, State.STOPPING)
      && !state.compareAndSet(State.ALIVE, State.STOPPING)
      && !state.compareAndSet(State.SUSPENDED, State.STOPPING)) {
      CompletableFuture<ProgramController> f = new CompletableFuture<>();
      f.completeExceptionally(new IllegalStateException("Stopping not allowed for " + name + " in " + state.get()));
      return f;
    }
    CompletableFuture<ProgramController> result = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        caller.stopping();
        doStop();
        state.set(State.KILLED);
        result.complete(AbstractProgramController.this);
        caller.killed();
      } catch (Throwable t) {
        error(t, result);
      }
    });
    return result;
  }

  /**
   * Children call this method to signal the program is completed.
   */
  protected void complete() {
    complete(State.COMPLETED);
  }

  protected void complete(final State completionState) {
    LOG.trace("Program {} completed with state {}", programId, completionState);
    if (!state.compareAndSet(State.STARTING, completionState)
      && !state.compareAndSet(State.ALIVE, completionState)
      && !state.compareAndSet(State.SUSPENDED, completionState)) {
      LOG.warn("Cannot transit to COMPLETED state from {} state: {}", state.get(), name);
      return;
    }
    executor.execute(() -> {
      state.set(completionState);
      if (State.KILLED.equals(completionState)) {
        caller.killed();
      } else {
        caller.completed();
      }
    });
  }

  @Override
  public final Cancellable addListener(Listener listener, final Executor listenerExecutor) {
    if (listener == null) {
      throw new NullPointerException("Listener shouldn't be null.");
    }
    if (listenerExecutor == null) {
      throw new NullPointerException("Executor shouldn't be null.");
    }

    final ListenerCaller caller = new ListenerCaller(listener, listenerExecutor);
    final Cancellable cancellable = () -> {
      // Simply remove the listener from the map through the executor and block on the completion
      Future f = executor.submit(() -> listeners.remove(caller));
      try {
        getUninterruptibly(f);
      } catch (ExecutionException e) {
        // should never never happen
        throw new RuntimeException(e.getCause());
      }
    };

    try {
      // Use a synchronous queue to communicate the Cancellable to return
      final SynchronousQueue<Cancellable> result = new SynchronousQueue<>();

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
      Throwable t = getRootCause(e);
      if (t instanceof RuntimeException) {
        throw (RuntimeException) t;
      } else {
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public final Future<ProgramController> command(final String name, final Object value) {
    CompletableFuture<ProgramController> result = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        doCommand(name, value);
        result.complete(AbstractProgramController.this);
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
   * @param t The failure cause
   */
  protected final void error(final Throwable t) {
    executor.execute(() -> error(t, null));
  }

  /**
   * Children call this method to signal the program is started.
   */
  protected final void started() {
    LOG.trace("Program {} started", programId);
    if (!state.compareAndSet(State.STARTING, State.ALIVE)) {
      LOG.debug("Cannot transit to ALIVE state from {} state: {}", state.get(), name);
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
   * Force this controller into error state and set the failure into the given future.
   * This method should only be called from the single thread executor of this class.
   * @param t The failure cause
   */
  private <V> void error(Throwable t, CompletableFuture<V> future) {
    LOG.trace("Program {} forced to error", programId, t);
    failureCause = t;
    state.set(State.ERROR);
    if (future != null) {
      future.completeExceptionally(t);
    }
    caller.error(t);
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause;
    while ((cause = throwable.getCause()) != null) {
      throwable = cause;
    }
    return throwable;
  }

  /**
   * Invokes {@code future.}{@link Future#get() get()} uninterruptibly.
   */
  private static <V> V getUninterruptibly(Future<V> future)
    throws ExecutionException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
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
        String msg = String.format("Exception while executing method '%s' on listener %s with executor %s.",
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
      return Objects.equals(listener, other.listener);
    }

    @Override
    public int hashCode() {
      return Objects.hash(listener);
    }
  }
}
