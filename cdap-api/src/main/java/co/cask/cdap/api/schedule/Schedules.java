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
    private Integer maxConcurrentRuns;

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
     * Set the maximum number of concurrent runs for the schedule. Prior to running the program,
     * the scheduler will check how many active runs of the scheduled program exist.
     * An active run is one that is not completed, failed, or killed. This includes suspended runs.
     * If that number is greater than or equal to the max, the scheduler will not run the program.
     * Program runs started manually or by another schedule are not included in this number.
     * For example, if the threshold is set to 1, the scheduler will
     * only run the program if there are no active runs started by this schedule. If another schedule started
     * the same program, it would not be factored into the decision to start a new run.
     *
     * If no threshold is set, there will be no limit on the number of concurrent runs.
     *
     * @param max skip the scheduled run if the number of concurrent program runs is above this threshold
     * @return the Builder
     */
    public Builder setMaxConcurrentRuns(int max) {
      if (max < 1) {
        throw new IllegalArgumentException("max concurrent runs must be at least 1.");
      }
      this.maxConcurrentRuns = max;
      return this;
    }

    /**
     * Create a time schedule based on the specified cron expression.
     *
     * @param cronExpression the cron expression specifying when the scheduler should attempt to start a run
     * @return the time schedule
     */
    public Schedule createTimeSchedule(String cronExpression) {
      return new TimeSchedule(name, description, cronExpression, new RunConstraints(maxConcurrentRuns));
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
          return new StreamSizeSchedule(name, description, sourceName, dataTriggerMB,
                                        new RunConstraints(maxConcurrentRuns));
      }
      throw new IllegalArgumentException("Unhandled source of " + source);
    }
  }
}
