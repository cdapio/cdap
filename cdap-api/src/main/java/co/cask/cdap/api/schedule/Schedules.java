/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;

/**
 * Factory class to create {@link Schedule} objects.
 */
public final class Schedules {
  
  /**
   * Build a time-based schedule.
   *
   * @param name name of the schedule
   * @param description description of the schedule
   * @param cronExpression cron expression for the schedule
   * @return a schedule based on the given {@code cronExpression}
   */
  public static Schedule createTimeSchedule(String name, String description, String cronExpression) {
    return new TimeSchedule(name, description, cronExpression);
  }

  /**
   * Build a schedule based on data availability.
   *
   * @param name name of the schedule
   * @param description description of the schedule
   * @param source source of data the schedule is based on
   * @param sourceName name of the source of data the schedule is based on
   * @param dataTriggerMB the size of data, in MB, that the source has to receive to trigger an execution
   * @return a schedule based on data availability in the given {@code dataSourceName}
   */
  public static Schedule createDataSchedule(String name, String description, Source source, String sourceName,
                                            int dataTriggerMB) {
    switch (source) {
      case STREAM:
        return new StreamSizeSchedule(name, description, sourceName, dataTriggerMB);
    }
    throw new IllegalArgumentException();
  }

  private Schedules() {
  }

  /**
   * Defines different types of data schedules.
   */
  public enum Source {
    STREAM
  }
}
