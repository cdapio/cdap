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


import java.util.Objects;

/**
 * Defines a cron-based schedule for running a program.
 */
public class Schedule {

  private final String name;

  private final String description;

  private final RunConstraints runConstraints;

  protected Schedule(String name, String description, RunConstraints runConstraints) {
    this.name = name;
    this.description = description;
    this.runConstraints = runConstraints;
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

  public RunConstraints getRunConstraints() {
    // need this null check for backwards compatibility. Schedules saved prior to v3.3 will not have it.
    return runConstraints == null ? RunConstraints.NONE : runConstraints;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Schedule that = (Schedule) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(getRunConstraints(), that.getRunConstraints());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, getRunConstraints());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Schedule{");
    sb.append("name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", runConstraints='").append(getRunConstraints()).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
