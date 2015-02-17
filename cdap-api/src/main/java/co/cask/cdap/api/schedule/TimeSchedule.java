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

/**
 * Defines a cron-based schedule for running a program.
 */
public final class TimeSchedule extends Schedule {

  private final String cronExpression;

  TimeSchedule(String name, String description, String cronExpression) {
    super(name, description);
    this.cronExpression = cronExpression;
  }

  /**
   * @return Cron expression for the schedule, if this schedule is a time based schedule.
   */
  public String getCronExpression() {
    return cronExpression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeSchedule schedule = (TimeSchedule) o;

    if (cronExpression.equals(schedule.cronExpression)
      && getDescription().equals(schedule.getDescription())
      && getName().equals(schedule.getName())) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = 31 * result + getDescription().hashCode();
    result = 31 * result + cronExpression.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TimeSchedule{");
    sb.append("name='").append(getName()).append('\'');
    sb.append(", description='").append(getDescription()).append('\'');
    sb.append(", cronExpression='").append(cronExpression).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
