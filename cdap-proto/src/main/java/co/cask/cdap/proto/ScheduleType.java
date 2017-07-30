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

package co.cask.cdap.proto;

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;

/**
 * Schedule Type.
 *
 * @deprecated as of 4.2.0. Use {@link co.cask.cdap.api.schedule.Trigger} instead.
 */
@Deprecated
public enum ScheduleType {
  /**
   * Represents {@link TimeSchedule} objects.
   */
  TIME,

  /**
   * Represents {@link StreamSizeSchedule} objects.
   */
  STREAM;

  public static ScheduleType fromSchedule(Schedule schedule) {
    if (schedule instanceof StreamSizeSchedule) {
      return STREAM;
    } else if (schedule instanceof TimeSchedule) {
      return TIME;
    } else {
      throw new IllegalArgumentException("Unhandled type of schedule: " + schedule.getClass());
    }
  }
}
