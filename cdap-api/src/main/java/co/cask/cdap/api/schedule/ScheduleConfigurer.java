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

import java.util.Map;
import java.util.TimeZone;

/**
 * Configurer for scheduling a {@link co.cask.cdap.api.workflow.Workflow}. The schedule must be triggered by a certain
 * event and wait for all constraints to be satisfied to launch the Workflow. Set the schedule's trigger with
 * methods {@link #triggerByTime(String)} or {@link #triggerOnPartitions(String, int)}.
 * Once the schedule's trigger is set, the schedule will be added to the app with all of the information
 * set in this configurer. To set the constraints for the schedule,
 * use methods {@link #limitConcurrentRuns(int)}, {@link #delayRun(long)}, {@link #setTimeRange(String, String)},
 * {@link #setTimeRange(String, String, TimeZone)}, and {@link #setDurationSinceLastRun(long)}.
 * If no constraint is specified, the schedule will immediately launch the Workflow once the schedule is triggered.
 */
public interface ScheduleConfigurer {

  /**
   * Set the description of the schedule.
   *
   * @param description given description of the schedule
   * @return {@link ScheduleConfigurer} containing the given description
   */
  ScheduleConfigurer setDescription(String description);

  /**
   * Set properties of the schedule.
   *
   * @param properties a {@link Map} with property names of class {@link String} as keys and
   *                   property values of class {@link String} as values
   * @return {@link ScheduleConfigurer} containing the given properties
   */
  ScheduleConfigurer setProperties(Map<String, String> properties);

  /**
   * Set the max number of concurrently running {@link co.cask.cdap.api.workflow.Workflow}
   * can be launched by this schedule.
   *
   * @param max the max number of concurrently running Workflows allowed to be launched by this schedule
   * @return {@link ScheduleConfigurer} containing the given max number of concurrent runs
   */
  ScheduleConfigurer limitConcurrentRuns(int max);

  /**
   * Set a certain amount of delay passed after the schedule is triggered, before launching the Workflow.
   *
   * @param delayMillis delay in milliseconds to wait after the schedule is triggered before launching the Workflow
   * @return {@link ScheduleConfigurer} containing the given delay
   */
  ScheduleConfigurer delayRun(long delayMillis);

  /**
   * Set a time range in a day starting from {@code startTime} and ending at {@code endTime}, between which
   * the Workflow is allowed to be launched. {@code endTime} must be later than {@code startTime}.
   * JVM's default time zone will be used to interpret {@code startTime} and {@code endTime}.
   *
   * @param startTime the start time (inclusive) in the format of "HH:mm", for instance 2am should be "02:00"
   * @param endTime the end time (exclusive) in the format of "HH:mm", for instance 5pm should be "17:00"
   * @return {@link ScheduleConfigurer} containing the time range
   */
  ScheduleConfigurer setTimeRange(String startTime, String endTime);

  /**
   * Set a time range in a day starting from {@code startTime} and ending at {@code endTime} in the given time zone,
   * between which the Workflow is allowed to be launched. {@code endTime} must be later than {@code startTime}.
   *
   * @param startTime the start time (inclusive) in the format of "HH:mm", for instance 2am should be "02:00"
   * @param endTime the end time (exclusive) in the format of "HH:mm", for instance 5pm should be "17:00"
   * @param timeZone the time zone of {@code startTime} and {@code endTime}
   * @return {@link ScheduleConfigurer} containing the time range
   */
  ScheduleConfigurer setTimeRange(String startTime, String endTime, TimeZone timeZone);

  /**
   * Set a certain duration passed since the last launching of the Workflow by this schedule,
   * before launching the Workflow again.
   *
   * @param delayMillis duration in milliseconds to wait after the last launching of the Workflow by this schedule
   *                    before launching the Workflow
   * @return {@link ScheduleConfigurer} containing the given duration
   */
  ScheduleConfigurer setDurationSinceLastRun(long delayMillis);

  /**
   * Create a schedule which is triggered whenever time matching the given cron expression comes.
   * After calling this method, the schedule will be added to the app with all of the information
   * set in this configurer.
   *
   * @param cronExpression the cron expression to specify the time to trigger the schedule
   */
  void triggerByTime(String cronExpression);

  /**
   * Create a schedule which is triggered whenever at least a certain number of new partitions
   * are added to a certain dataset. After calling this method, the schedule will be added to
   * the app with all of the information set in this configurer.
   *
   * @param datasetName the name of the dataset in the same namespace of the app
   * @param numPartitions the minimum number of new partitions added to the dataset to trigger the schedule
   */
  void triggerOnPartitions(String datasetName, int numPartitions);
}
