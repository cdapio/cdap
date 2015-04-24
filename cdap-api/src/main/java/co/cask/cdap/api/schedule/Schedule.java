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
   * @deprecated As of version 2.8.0, do not use this method anymore
   */
  @Deprecated
  public String getCronEntry() {
    return cronEntry;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Schedule schedule = (Schedule) o;

    if (cronEntry != null
          ? !cronEntry.equals(schedule.cronEntry)
          : schedule.cronEntry != null) {
      return false;
    }
    if (description != null ? !description.equals(schedule.description) :
         schedule.description != null) {
      return false;
    }
    if (name != null ? !name.equals(schedule.name) : schedule.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (description != null ? description.hashCode() : 0);
    result = 31 * result + (cronEntry != null ? cronEntry.hashCode() : 0);
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
