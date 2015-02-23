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

package co.cask.cdap.app.runtime;

import co.cask.cdap.proto.ProgramRunStatus;
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
    STARTING(ProgramRunStatus.RUNNING),

    /**
     * Program is alive.
     */
    ALIVE(ProgramRunStatus.RUNNING),

    /**
     * Trying to suspend the program.
     */
    SUSPENDING(ProgramRunStatus.RUNNING),

    /**
     * Program is suspended.
     */
    SUSPENDED(ProgramRunStatus.RUNNING),

    /**
     * Trying to resume a suspended program.
     */
    RESUMING(ProgramRunStatus.RUNNING),

    /**
     * Trying to stop a program.
     */
    STOPPING(ProgramRunStatus.RUNNING),

    /**
     * Program stopped. It is a terminal state, no more state transition is allowed.
     */
    STOPPED(ProgramRunStatus.COMPLETED),

    /**
     * Program runs into error. It is a terminal state, no more state transition is allowed.
     */
    ERROR(ProgramRunStatus.FAILED);

    private final ProgramRunStatus runStatus;

    private State(ProgramRunStatus runStatus) {
      this.runStatus = runStatus;
    }

    /**
     * Gives the {@link ProgramController.State} for a given {@link ProgramRunStatus}
     * @param status : the status
     * @return the state for the status or null if there is no defined state for the given status
     */
    public static State getControllerStateByStatus (ProgramRunStatus status) {
      for (ProgramController.State state : ProgramController.State.values()) {
        if (state.getRunStatus() == status) {
          return state;
        }
      }
      return null;
    }
    
    public ProgramRunStatus getRunStatus() {
      return runStatus;
    }
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
   * @return The failure cause of the program if the current state is {@link State#ERROR}, otherwise
   *         {@code null} will be returned.
   */
  Throwable getFailureCause();

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
     * Called when the listener is added. This method will triggered once only and triggered before any other
     * method in this interface is called.
     *
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
