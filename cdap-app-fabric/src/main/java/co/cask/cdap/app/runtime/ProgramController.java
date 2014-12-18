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

import co.cask.cdap.proto.ProgramState;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.Executor;

/**
 *
 */
public interface ProgramController {

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
  ProgramState getState();

  /**
   * @return The failure cause of the program if the current state is {@link ProgramState#FAILED}, otherwise
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
    void init(ProgramState currentState);

    void suspending();

    void suspended();

    void resuming();

    void alive();

    void stopping();

    void stopped();

    void error(Throwable cause);
  }
}
