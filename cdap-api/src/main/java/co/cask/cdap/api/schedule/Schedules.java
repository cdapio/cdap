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
    throw new IllegalArgumentException("Unhandled source of " + source);
  }

  private Schedules() {
  }

  /**
   * Defines different types of data schedules.
   */
  public enum Source {
    STREAM
  }

  /**
   * Get a builder used to create a schedule.
   *
   * @param name the name of the schedule to build
   * @return a builder used to create a schedule
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * Builder used to create a schedule.
   */
  public static class Builder {
    private final String name;
    private String description;
    private Integer concurrentProgramRunsThreshold;

    private Builder(String name) {
      this.name = name;
      this.description = "";
    }

    /**
     * Set the schedule description.
     *
     * @param description the schedule description
     * @return the Builder
     */
    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    /**
     * Skip running the scheduled program if the number of concurrent runs of the program exceeds the specified
     * threshold. Prior to running the program, the scheduler will check how many runs of the program are currently
     * in the RUNNING state.
     * If that number is greater than the specified threshold, the scheduler will not run the program.
     * This number includes runs of the program that were started outside of this schedule,
     * either manually or by another schedule. For example, if the threshold is set to 0, the scheduler will
     * only run the program if there are no other runs of the program currently in the RUNNING state.
     *
     * If no threshold is set, there will be no limit on the number of concurrent runs.
     *
     * @param threshold skip the scheduled run if the number of concurrent program runs is above this threshold
     * @return the Builder
     */
    public Builder skipIfConcurrentProgramRunsExceed(int threshold) {
      if (threshold < 0) {
        throw new IllegalArgumentException("concurrent program runs threshold must be at least 0.");
      }
      this.concurrentProgramRunsThreshold = threshold;
      return this;
    }

    /**
     * Create a time schedule based on the specified cron expression.
     *
     * @param cronExpression the cron expression specifying when the scheduler should attempt to start a run
     * @return the time schedule
     */
    public Schedule createTimeSchedule(String cronExpression) {
      return new TimeSchedule(name, description, cronExpression, new RunRequirements(concurrentProgramRunsThreshold));
    }

    /**
     * Create a data schedule that runs where a certain amount of data is available.
     *
     * @param source source of data the schedule is based on
     * @param sourceName name of the source of data the schedule is based on
     * @param dataTriggerMB the size of data, in MB, that the source has to receive to trigger an execution
     * @return a schedule based on data availability in the given {@code dataSourceName}
     */
    public Schedule createDataSchedule(Source source, String sourceName, int dataTriggerMB) {
      switch (source) {
        case STREAM:
          return new StreamSizeSchedule(name, description, sourceName, dataTriggerMB);
      }
      throw new IllegalArgumentException("Unhandled source of " + source);
    }
  }
}
