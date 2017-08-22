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
 * The time trigger information to be passed to the triggered program.
 */
public interface TimeTriggerInfo extends TriggerInfo {

  /**
   * @return The cron expression in the time trigger.
   */
  String getCronExpression();

  /**
   * Returns the logical start time of the triggered program. Logical start time is when the schedule decides to launch
   * the program when the cron expression is satisfied.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC)
   */
  long getLogicalStartTime();
}
