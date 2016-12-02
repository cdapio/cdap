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
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Abstract base implementation of {@link ProgramController} that governs state transitions as well as
 * {@link Listener} invocation mechanism.
 */
public abstract class AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramController.class);

  private final AtomicReference<State> state;
  private final ProgramId programId;
  private final RunId runId;
  private final String componentName;
  private final Map<ListenerCaller, Cancellable> listeners;
  private final Listener caller;
  private final ExecutorService executor;
  private final String name;

  private Throwable failureCause;

  protected AbstractProgramController(final ProgramId programId, RunId runId) {
    this(programId, runId, null);
  }

  protected AbstractProgramController(ProgramId programId, RunId runId, @Nullable String componentName) {
    this.state = new AtomicReference<>(State.STARTING);
    this.programId = programId;
    this.runId = runId;
    this.componentName = componentName;
    this.listeners = new HashMap<>();
    this.caller = new MultiListenerCaller();
    this.name = programId + (componentName == null ? "" : "-" + componentName) + "-" + runId.getId();

    // Create a single thread executor that doesn't keep core thread and the thread will shutdown when there
    // is no pending task. In this way, we don't need to shutdown the executor since there will be no thread
    // hanging around when it is idle.
    this.executor = new ThreadPoolExecutor(0, 1, 0, TimeUnit.SECONDS,
                                           new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(null, r, "pcontroller-" + name);
      }
    });
  }

  @Override
  public ProgramId getProgramId() {
    return programId;
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
  public final ListenableFuture<ProgramController> suspend() {
    if (!state.compareAndSet(State.ALIVE, State.SUSPENDING)) {
      return Futures.immediateFailedFuture(
        new IllegalStateException("Suspension not allowed for " + programId + " in " + state.get()));
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor.execute(new Runnable() {
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
      return Futures.immediateFailedFuture(
        new IllegalStateException("Resumption not allowed for " + name + " in " + state.get()));
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor.execute(new Runnable() {
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
  public final ListenableFuture<ProgramController> stop() {
    if (!state.compareAndSet(State.STARTING, State.STOPPING)
      && !state.compareAndSet(State.ALIVE, State.STOPPING)
      && !state.compareAndSet(State.SUSPENDED, State.STOPPING)) {
      return Futures.immediateFailedFuture(
        new IllegalStateException("Stopping not allowed for " + name + " in " + state.get()));
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          caller.stopping();
          doStop();
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

  /**
   * Children call this method to signal the program is completed.
   */
  protected void complete() {
    complete(State.COMPLETED);
  }

  protected void complete(final State completionState) {
    if (!state.compareAndSet(State.STARTING, completionState)
      && !state.compareAndSet(State.ALIVE, completionState)
      && !state.compareAndSet(State.SUSPENDED, completionState)) {
      LOG.warn("Cannot transit to COMPLETED state from {} state: {} {}", state.get(), name);
      return;
    }
    executor.execute(new Runnable() {
      @Override
      public void run() {
        state.set(completionState);
        if (State.KILLED.equals(completionState)) {
          caller.killed();
        } else {
          caller.completed();
        }
      }
    });
  }

  @Override
  public final Cancellable addListener(Listener listener, final Executor listenerExecutor) {
    Preconditions.checkNotNull(listener, "Listener shouldn't be null.");
    Preconditions.checkNotNull(listenerExecutor, "Executor shouldn't be null.");

    final ListenerCaller caller = new ListenerCaller(listener, listenerExecutor);
    final Cancellable cancellable = new Cancellable() {
      @Override
      public void cancel() {
        // Simply remove the listener from the map through the executor and block on the completion
        Futures.getUnchecked(executor.submit(new Runnable() {
          @Override
          public void run() {
            listeners.remove(caller);
          }
        }));
      }
    };

    try {
      // Use a synchronous queue to communicate the Cancellable to return
      final SynchronousQueue<Cancellable> result = new SynchronousQueue<>();

      // Use the single thread executor to add the listener and call init
      executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Cancellable existing = listeners.get(caller);
          if (existing == null) {
            listeners.put(caller, cancellable);
            result.put(cancellable);
            caller.init(getState(), getFailureCause());
          } else {
            result.put(existing);
          }
          return null;
        }
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
    executor.execute(new Runnable() {

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

  /**
   * Force this controller into error state.
   * @param t The failure cause
   */
  protected final void error(final Throwable t) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        error(t, null);
      }
    });
  }

  /**
   * Children call this method to signal the program is started.
   */
  protected final void started() {
    if (!state.compareAndSet(State.STARTING, State.ALIVE)) {
      LOG.debug("Cannot transit to ALIVE state from {} state: {}", state.get(), name);
      return;
    }
    executor.execute(new Runnable() {
      @Override
      public void run() {
        state.set(State.ALIVE);
        caller.alive();
      }
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
  private <V> void error(Throwable t, SettableFuture<V> future) {
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

    @Override
    public void init(final State currentState, @Nullable final Throwable cause) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.init(currentState, cause);
        }
      });
    }

    @Override
    public void suspending() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.suspending();
        }
      });
    }

    @Override
    public void suspended() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.suspended();
        }
      });
    }

    @Override
    public void resuming() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.resuming();
        }
      });
    }

    @Override
    public void alive() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.alive();
        }
      });
    }

    @Override
    public void stopping() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.stopping();
        }
      });
    }

    @Override
    public void completed() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.completed();
        }
      });
    }

    @Override
    public void killed() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.killed();
        }
      });
    }

    @Override
    public void error(final Throwable cause) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          listener.error(cause);
        }
      });
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
