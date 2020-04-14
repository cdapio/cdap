/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime;

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.Executor;
import javax.annotation.Nullable;

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
   * ALIVE ---------------> COMPLETED   |   |
   *   |   -> STOPPING ---> KILLED      |   |
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
    STARTING(ProgramRunStatus.STARTING),

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
    SUSPENDED(ProgramRunStatus.SUSPENDED),

    /**
     * Trying to resume a suspended program.
     */
    RESUMING(ProgramRunStatus.RESUMING),

    /**
     * Trying to stop a program.
     */
    STOPPING(ProgramRunStatus.RUNNING),

    /**
     * Program completed. It is a terminal state, no more state transition is allowed.
     */
    COMPLETED(ProgramRunStatus.COMPLETED),

    /**
     * Program was killed by user.
     */
    KILLED(ProgramRunStatus.KILLED),

    /**
     * Program runs into error. It is a terminal state, no more state transition is allowed.
     */
    ERROR(ProgramRunStatus.FAILED);

    private final ProgramRunStatus runStatus;

    State(ProgramRunStatus runStatus) {
      this.runStatus = runStatus;
    }

    public ProgramRunStatus getRunStatus() {
      return runStatus;
    }

    public boolean isDone() {
      return this == COMPLETED || this == KILLED || this == ERROR;
    }
  }

  /**
   * Returns the program run id which this controller is controlling.
   */
  ProgramRunId getProgramRunId();

  /**
   * Returns the run Id which this controller is controlling.
   */
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
   * @param listener listener for listening to state changes
   * @param executor the executor used for making calls to the given listener
   * @return a {@link Cancellable} to cancel the listening
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
     * @param cause The cause of failure if the program failed by the time when the listener is added.
     */
    void init(State currentState, @Nullable Throwable cause);

    void suspending();

    void suspended();

    void resuming();

    void alive();

    void stopping();

    void completed();

    void killed();

    void error(Throwable cause);
  }
}
