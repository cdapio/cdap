package com.continuuity.app.runtime;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.Executor;

/**
 *
 */
public interface ProgramController {

  /**
   * All possible state transitions.
   * <p/>
   * <pre>
   *
   *
   *   |------------------------------------|
   *   |                                    |
   *   |        |-----------------------|   |
   *   v        v                       |   |
   * ALIVE -> STOPPING ---> STOPPED     |   |
   *   |                                |   |
   *   |----> SUSPENDING -> SUSPENDED --|   |
   *                            |           |
   *                            |------> RESUMING
   *
   * </pre>
   * <p>
   *   Any error during state transitions would end up in ERROR state.
   * </p>
   * <p>
   *   Any unrecoverable error in ALIVE state would also end up in ERROR state.
   * </p>
   */
  enum State {

    /**
     * Program is starting.
     */
    STARTING,

    /**
     * Program is alive.
     */
    ALIVE,

    /**
     * Trying to suspend the program.
     */
    SUSPENDING,

    /**
     * Program is suspended.
     */
    SUSPENDED,

    /**
     * Trying to resume a suspended program.
     */
    RESUMING,

    /**
     * Trying to stop a program.
     */
    STOPPING,

    /**
     * Program stopped. It is a terminal state, no more state transition is allowed.
     */
    STOPPED,

    /**
     * Program runs into error. It is a terminal state, no more state transition is allowed.
     */
    ERROR
  }

  RunId getRunId();

  /**
   * Suspend the running {@link ProgramRunner}.
   * @return A {@link ListenableFuture} that will be completed when the program is actually suspended.
   */
  ListenableFuture<ProgramController> suspend();

  ListenableFuture<ProgramController> resume();

  ListenableFuture<ProgramController> stop();

  /**
   * @return The current state of the program at the time when this method is called.
   */
  State getState();

  /**
   * Adds a listener to watch for state changes. Adding the same listener again don't have any effect
   * and simply will get the same {@link Cancellable} back.
   * @param listener
   * @param executor
   * @return
   */
  Cancellable addListener(Listener listener, Executor executor);

  /**
   * Sends a command to the program. It's up to the program on how to handle it.
   * @param name Name of the command.
   * @param value Value of the command.
   * @return A {@link ListenableFuture} that would be completed when the command is handled or ignored.
   */
  ListenableFuture<ProgramController> command(String name, Object value);

  /**
   * Listener for getting callbacks on state changed on {@link ProgramController}.
   */
  interface Listener {
    /**
     * Called when the listener is added. This method will triggered once only.
     * @param currentState The state of the program by the time when the listener is added.
     */
    void init(State currentState);

    void suspending();

    void suspended();

    void resuming();

    void alive();

    void stopping();

    void stopped();

    void error(Throwable cause);
  }
}
