/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
 * Defines a cron-based schedule for running a program.
 */
public class Schedule {

  private final String name;

  private final String description;

  // NOTE: the two below attributes are left for backwards compatibility
  private final String cronEntry;

  /**
   * Build a time-based schedule.
   *
   * @param name name of the schedule
   * @param description description of the schedule
   * @param cronExpression cron expression for the schedule
   * @return a schedule based on the given {@code cronExpression}
   */
  public static Schedule buildTimeSchedule(String name, String description, String cronExpression) {
    return new TimeSchedule(name, description, cronExpression);
  }

  /**
   * Build a schedule based on data availability in a stream.
   * @param name name of the schedule
   * @param description description of the schedule
   * @param streamName name of the stream the schedule is based on
   * @param dataTriggerMB the size of data, in MB, that the stream has to receive to trigger an execution
   * @return a schedule based on data availability in the given {@code streamName}
   */
  public static Schedule buildStreamSizeSchedule(String name, String description, String streamName,
                                                           int dataTriggerMB) {
    return new StreamSizeSchedule(name, description, streamName, dataTriggerMB);
  }

  @Deprecated
  public Schedule(String name, String description, String cronEntry) {
    this.name = name;
    this.description = description;
    this.cronEntry = cronEntry;
  }

  Schedule(String name, String description) {
    this.name = name;
    this.description = description;
    this.cronEntry = null;
  }

  /**
   * @return Name of the schedule.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Schedule description.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return Cron expression for the schedule.
   */
  @Deprecated
  public String getCronEntry() {
    return cronEntry;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Schedule schedule = (Schedule) o;

    if (cronEntry.equals(schedule.cronEntry) && description.equals(schedule.description)
      && name.equals(schedule.name)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + description.hashCode();
    result = 31 * result + cronEntry.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Schedule{");
    sb.append("name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", cronEntry='").append(cronEntry).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
