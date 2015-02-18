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


import co.cask.cdap.internal.schedule.TimeSchedule;

/**
 * Defines a cron-based schedule for running a program.
 */
public class Schedule {

  private final String name;

  private final String description;

  // NOTE: the below attribute is left for backwards compatibility
  private final String cronEntry;

  @Deprecated
  public Schedule(String name, String description, String cronEntry) {
    this.name = name;
    this.description = description;
    this.cronEntry = cronEntry;
  }

  protected Schedule(String name, String description) {
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
   * @deprecated As of version 2.8.0, use {@link TimeSchedule#getCronEntry()} instead
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
