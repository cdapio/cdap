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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.ProgramController;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract base implementation of {@link ProgramController} that governs state transitions as well as
 * {@link Listener} invocation mechanism.
 */
public abstract class AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramController.class);

  private final AtomicReference<State> state;
  private final String programName;
  private final RunId runId;
  private final ConcurrentMap<ListenerCaller, Cancellable> listeners;
  private final Listener caller;
  private Throwable failureCause;

  protected AbstractProgramController(String programName, RunId runId) {
    this.state = new AtomicReference<State>(State.STARTING);
    this.programName = programName;
    this.runId = runId;
    this.listeners = Maps.newConcurrentMap();
    this.caller = new MultiListenerCaller();
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public final ListenableFuture<ProgramController> suspend() {
    if (!state.compareAndSet(State.ALIVE, State.SUSPENDING)) {
      return Futures.immediateFailedFuture(new IllegalStateException("Suspension not allowed").fillInStackTrace());
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor(State.SUSPENDING).execute(new Runnable() {
      @Override
      public void run() {
        try {
          caller.suspending();
          doSuspend();
          state.set(State.SUSPENDED);
          result.set(AbstractProgramController.this);
          caller.suspended();
        } catch (Throwable t) {
          error(t, result);
        }
      }
    });

    return result;
  }

  @Override
  public final ListenableFuture<ProgramController> resume() {
    if (!state.compareAndSet(State.SUSPENDED, State.RESUMING)) {
      return Futures.immediateFailedFuture(new IllegalStateException("Resumption not allowed").fillInStackTrace());
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor(State.RESUMING).execute(new Runnable() {
      @Override
      public void run() {
        try {
          caller.resuming();
          doResume();
          state.set(State.ALIVE);
          result.set(AbstractProgramController.this);
          caller.alive();
        } catch (Throwable t) {
          error(t, result);
        }
      }
    });
    return result;
  }

  @Override
  public final ListenableFuture<ProgramController> complete() {
    if (!state.compareAndSet(State.STARTING, State.STOPPING)
      && !state.compareAndSet(State.ALIVE, State.STOPPING)
      && !state.compareAndSet(State.SUSPENDED, State.STOPPING)) {
      return Futures.immediateFailedFuture(new IllegalStateException("Stopping not allowed").fillInStackTrace());
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor(State.STOPPING).execute(new Runnable() {
      @Override
      public void run() {
        try {
          caller.stopping();
          doComplete();
          state.set(State.COMPLETED);
          result.set(AbstractProgramController.this);
          caller.completed();
        } catch (Throwable t) {
          error(t, result);
        }
      }
    });
    return result;
  }

  @Override
  public final ListenableFuture<ProgramController> kill() {
    if (!state.compareAndSet(State.STARTING, State.STOPPING)
      && !state.compareAndSet(State.ALIVE, State.STOPPING)
      && !state.compareAndSet(State.SUSPENDED, State.STOPPING)) {
      return Futures.immediateFailedFuture(new IllegalStateException("Stopping not allowed").fillInStackTrace());
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor(State.STOPPING).execute(new Runnable() {
      @Override
      public void run() {
        try {
          caller.stopping();
          doKill();
          state.set(State.KILLED);
          result.set(AbstractProgramController.this);
          caller.killed();
        } catch (Throwable t) {
          error(t, result);
        }
      }
    });
    return result;
  }

  @Override
  public final Cancellable addListener(Listener listener, Executor executor) {
    Preconditions.checkNotNull(listener, "Listener shouldn't be null.");
    Preconditions.checkNotNull(executor, "Executor shouldn't be null.");
    final ListenerCaller caller = new ListenerCaller(listener, executor, state.get());
    Cancellable cancellable = new Cancellable() {
      @Override
      public void cancel() {
        listeners.remove(caller);
      }
    };

    Cancellable result = listeners.putIfAbsent(caller, cancellable);
    if (result != null) {
      return result;
    }

    caller.init(state.get());
    return cancellable;
  }

  @Override
  public final ListenableFuture<ProgramController> command(final String name, final Object value) {
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor("command").execute(new Runnable() {

      @Override
      public void run() {
        try {
          doCommand(name, value);
          result.set(AbstractProgramController.this);
        } catch (Throwable t) {
          error(t, result);
        }
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

  protected final void error(Throwable t) {
    error(t, null);
  }

  /**
   * Force this controller into error state.
   * @param t The
   */
  protected final <V> void error(Throwable t, SettableFuture<V> future) {
    failureCause = t;
    state.set(State.ERROR);
    if (future != null) {
      future.setException(t);
    }
    caller.error(t);
  }

  /**
   * Children call this method to signal the program is started.
   */
  protected final void started() {
    if (!state.compareAndSet(State.STARTING, State.ALIVE)) {
      LOG.debug("Cannot transit to ALIVE state from {} state: {} {}", state.get(), programName, runId);
      return;
    }
    LOG.debug("Program started: {} {}", programName, runId);
    executor(State.ALIVE).execute(new Runnable() {
      @Override
      public void run() {
        state.set(State.ALIVE);
        caller.alive();
      }
    });
  }

  /**
   * Creates a new executor that execute using new thread everytime.
   */
  protected Executor executor(final String name) {
    return new Executor() {
      @Override
      public void execute(@Nonnull Runnable command) {
        Thread t = new Thread(command, programName + "-" + state);
        t.setDaemon(true);
        t.start();
      }
    };
  }

  protected abstract void doSuspend() throws Exception;

  protected abstract void doResume() throws Exception;

  protected abstract void doComplete() throws Exception;

  protected abstract void doKill() throws Exception;

  protected abstract void doCommand(String name, Object value) throws Exception;

  private Executor executor(State state) {
    return executor(state.name());
  }

  private final class MultiListenerCaller implements Listener {

    @Override
    public void init(State currentState) {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.init(currentState);
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

  private static final class ListenerCaller implements Listener, Runnable {

    private final Listener listener;
    private final Executor executor;
    private final State initState;
    private final Queue<ListenerTask> tasks;
    private State lastState;

    private ListenerCaller(Listener listener, Executor executor, State initState) {
      this.listener = listener;
      this.executor = executor;
      this.initState = initState;
      this.tasks = new LinkedList<ListenerTask>();
    }

    @Override
    public void init(final State currentState) {
      // The init state is being passed from constructor, hence ignoring the state passed to this method
      addTask(null);
    }

    @Override
    public void suspending() {
      addTask(State.SUSPENDING);
    }

    @Override
    public void suspended() {
      addTask(State.SUSPENDED);
    }

    @Override
    public void resuming() {
      addTask(State.RESUMING);
    }

    @Override
    public void alive() {
      addTask(State.ALIVE);
    }

    @Override
    public void stopping() {
      addTask(State.STOPPING);
    }

    @Override
    public void completed() {
      addTask(State.COMPLETED);
    }

    @Override
    public void killed() {
      addTask(State.KILLED);
    }

    @Override
    public void error(final Throwable cause) {
      addTask(State.ERROR, cause);
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

    @Override
    public void run() {
      while (true) {
        ListenerTask task;

        // Get the first task in the queue, don't dequeue so that the queue does not get emptied.
        synchronized (this) {
          task = tasks.peek();
        }

        // If no task in the queue, break the loop as there is nothing to execute
        if (task == null) {
          break;
        }

        // Trigger only if state actually changed
        if (!task.getState().equals(lastState)) {
          // Run the task without holding lock
          try {
            task.run();
          } catch (Throwable t) {
            LOG.warn(t.getMessage(), t);
          } finally {
            lastState = task.getState();
          }
        }

        // Dequeue the task that just processed and check if the queue is empty. These two operations need to be atomic.
        // Otherwise tasks may get double executed since addTask() uses isEmpty to determine if there is need
        // to submit task to executor.
        synchronized (this) {
          tasks.poll();
          if (tasks.isEmpty()) {
            break;
          }
        }
      }
    }

    /**
     * Adds a task to the end of the task queue.
     *
     * @param state State of the task. If {@code null}, the init state will be used.
     */
    private void addTask(@Nullable State state) {
      addTask(state, null);
    }

    /**
     * Adds a task to the end of the task queue.
     *
     * @param state state of the task. If {@code null}, the init state will be used.
     * @param failureCause cause of the error state if not {@code null}.
     */
    private void addTask(@Nullable State state, @Nullable Throwable failureCause) {
      boolean execute;
      State taskState = (state == null) ? initState : state;

      synchronized (this) {
        // Determine if there is need to submit task to executor and add the task to the queue.
        // These two steps need to be atomic.
        execute = tasks.isEmpty();
        tasks.add(new ListenerTask(listener, state == null, taskState, failureCause));
      }
      if (execute) {
        executor.execute(this);
      }
    }
  }

  /**
   * Represents a task to be executed as {@link Listener} callback.
   */
  private static final class ListenerTask implements Runnable {

    private final Listener listener;
    private final boolean initTask;
    private final State state;
    private final Throwable failureCause;

    private ListenerTask(Listener listener, boolean initTask, State state, @Nullable Throwable failureCause) {
      this.listener = listener;
      this.initTask = initTask;
      this.state = state;
      this.failureCause = failureCause;
    }

    public State getState() {
      return state;
    }

    @Override
    public void run() {
      if (initTask) {
        listener.init(state);
        return;
      }

      switch (state) {
        case STARTING:
          // No-op
          break;
        case ALIVE:
          listener.alive();
          break;
        case SUSPENDING:
          listener.suspending();
          break;
        case SUSPENDED:
          listener.suspended();
          break;
        case RESUMING:
          listener.resuming();
          break;
        case STOPPING:
          listener.stopping();
          break;
        case COMPLETED:
          listener.completed();
          break;
        case KILLED:
          listener.killed();
          break;
        case ERROR:
          listener.error(failureCause);
          break;
      }
    }
  }
}
