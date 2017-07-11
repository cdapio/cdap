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

package co.cask.cdap.app.runtime;

import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;

import javax.annotation.Nullable;

/**
 * An interface that defines the behavior for how program states are persisted
 */
public interface ProgramStateWriter {

  /**
   * Updates the program run's status to be {@link ProgramRunStatus#STARTING} at the given start time
   *
   * @param startTime the start time of the program in milliseconds when it has reached
   *                  {@link ProgramRunStatus#STARTING}
   */
  void start(long startTime);

  /**
   * Updates the program run's status to be {@link ProgramRunStatus#RUNNING} at the given start time in seconds
   *
   * @param startTimeInSeconds the start time of the program in seconds when it has reached
   *                           {@link ProgramRunStatus#RUNNING}
   */
  void running(long startTimeInSeconds);

  /**
   * Updates the program run's status to be terminated at the given time with the given run status
   *
   * @param endTime the end time of the program when it has terminated
   * @param runStatus the final run status of the program
   * @param cause the reason for the program run's failure, if the program terminated with an error
   */
  void stop(long endTime, ProgramRunStatus runStatus, @Nullable BasicThrowable cause);

  /**
   * Updates the program state as suspending
   */
  void suspend();

  /**
   * Updates the program run's status to be resumed
   *
   */
  void resume();
}