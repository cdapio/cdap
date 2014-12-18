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

package co.cask.cdap.proto;

/**
 * All possible state transitions of ProgramController.
 * <p/>
 * <pre>
 *
 *
 *   |------------------------------------|
 *   |                                    |
 *   |          |---------------------|   |
 *   v          v                     |   |
 * RUNNING -> STOPPING -> STOPPED     |   |
 *   |                                |   |
 *   |----> SUSPENDING -> SUSPENDED --|   |
 *                            |           |
 *                            |------> RESUMING
 *
 * </pre>
 * <p>
 *   Any error during state transitions would end up in FAILED state.
 * </p>
 * <p>
 *   Any unrecoverable error in RUNNING state would also end up in FAILED state.
 * </p>
 */
public enum ProgramState {

  /**
   * Program is starting.
   */
  STARTING(ProgramRunStatus.RUNNING),

  /**
   * Program is running.
   */
  RUNNING(ProgramRunStatus.RUNNING),

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
  FAILED(ProgramRunStatus.FAILED);

  private final ProgramRunStatus runStatus;

  private ProgramState(ProgramRunStatus runStatus) {
    this.runStatus = runStatus;
  }

  public ProgramRunStatus getRunStatus() {
    return runStatus;
  }

}
