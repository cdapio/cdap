package com.continuuity.internal.app.runtime;

import com.continuuity.app.runtime.ProgramController;
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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 *
 */
public abstract class AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramController.class);

  private final AtomicReference<State> state;
  private final String programName;
  private final RunId runId;
  private final ConcurrentMap<ListenerCaller, Cancellable> listeners;
  private final Listener caller;

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
  public final ListenableFuture<ProgramController> stop() {
    if (!state.compareAndSet(State.STARTING, State.STOPPING)
            && !state.compareAndSet(State.ALIVE, State.STOPPING)
            && !state.compareAndSet(State.SUSPENDED, State.STOPPING)) {
      return Futures.immediateFailedFuture(new IllegalStateException("Resumption not allowed").fillInStackTrace());
    }
    final SettableFuture<ProgramController> result = SettableFuture.create();
    executor(State.STOPPING).execute(new Runnable() {
      @Override
      public void run() {
        try {
          caller.stopping();
          doStop();
          state.set(State.STOPPED);
          result.set(AbstractProgramController.this);
          caller.stopped();
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
    final ListenerCaller caller = new ListenerCaller(listener, executor);
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

  protected final void error(Throwable t) {
    error(t, null);
  }

  /**
   * Force this controller into error state.
   * @param t The
   */
  protected final <V> void error(Throwable t, SettableFuture<V> future) {
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
      LOG.info("Program already started {} {}", programName, runId);
      return;
    }
    LOG.info("Program started: {} {}", programName, runId);
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

  protected abstract void doStop() throws Exception;

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
    public void stopped() {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.stopped();
      }
    }

    @Override
    public void error(Throwable cause) {
      for (ListenerCaller caller : listeners.keySet()) {
        caller.error(cause);
      }
    }
  }

  private static final class ListenerCaller implements Listener {
    private final Listener listener;
    private final Executor executor;

    private ListenerCaller(Listener listener, Executor executor) {
      this.listener = listener;
      this.executor = executor;
    }

    @Override
    public void init(final State currentState) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.init(currentState);
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
        }
      });
    }

    @Override
    public void suspending() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.suspending();
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
        }
      });
    }

    @Override
    public void suspended() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.suspended();
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
        }
      });
    }

    @Override
    public void resuming() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.resuming();
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
        }
      });
    }

    @Override
    public void alive() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.alive();
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
        }
      });
    }

    @Override
    public void stopping() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.stopping();
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
        }
      });
    }

    @Override
    public void stopped() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.stopped();
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
        }
      });
    }

    @Override
    public void error(final Throwable cause) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            listener.error(cause);
          } catch (Throwable t) {
            LOG.info(t.getMessage(), t);
          }
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
