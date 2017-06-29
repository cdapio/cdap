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

import co.cask.cdap.proto.ProgramRunStatus;
import org.apache.twill.api.RunId;

import javax.annotation.Nullable;

/**
 * An interface that defines the behavior for how program states are persisted
 */
public interface ProgramStateWriter {

  /**
   */
  void start(long startTimeInSeconds);

  /**
   *
   */
  void running(long startTimeInSeconds);

  /**
   *
   * @param runStatus
   * @param cause
   */
  void stop(long endTimeInSeconds, ProgramRunStatus runStatus, @Nullable Throwable cause);

  /**
   *
   */
  void suspend();

  /**
   *
   */
  void resume();
}
