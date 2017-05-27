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

package co.cask.cdap.api.schedule;

/**
 * Similar to {@link ScheduleBuilder}, but allows specifying whether the scheduler should wait until the
 * configured constraint is met.
 */
public interface ConstraintProgramScheduleBuilder extends ScheduleBuilder {

  /**
   * Specifies that the scheduler should wait until the configured constraint is met.
   */
  ScheduleBuilder waitUntilMet();

  /**
   * Specifies that the scheduler will abort the schedule execution if the configured constraint is not met.
   */
  ScheduleBuilder abortIfNotMet();
}
