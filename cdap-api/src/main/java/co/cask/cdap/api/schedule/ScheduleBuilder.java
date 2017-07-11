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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Builder for scheduling a program. The schedule must be triggered by a certain event and wait for all constraints to
 * be satisfied to launch the program. Build the schedule by setting the schedule's trigger with
 * methods {@link #triggerByTime(String)} or {@link #triggerOnPartitions(String, int)}.
 * To set the constraints for the schedule, use methods {@link #withConcurrency(int)},
 * {@link #withDelay(long, TimeUnit)}, {@link #withTimeWindow(String, String)},
 * {@link #withTimeWindow(String, String, TimeZone)}, and {@link #withDurationSinceLastRun(long, TimeUnit)}.
 * If no constraint is specified, the schedule will immediately launch the program once the schedule is triggered.
 */
public interface ScheduleBuilder {

  /**
   * Set the description of the schedule.
   *
   * @param description given description of the schedule
   * @return {@link ScheduleBuilder} containing the given description
   */
  ScheduleBuilder setDescription(String description);

  /**
   * Set properties of the schedule.
   *
   * @param properties a {@link Map} with property names of class {@link String} as keys and
   *                   property values of class {@link String} as values
   * @return {@link ScheduleBuilder} containing the given properties
   */
  ScheduleBuilder setProperties(Map<String, String> properties);

  /**
   * Sets a timeout on the schedule. Once a scheduled job is created and the specified number of seconds has passed,
   * the job will be removed without execute.
   * For instance, if the timeout is 21600 seconds (6 hours), and the configured constraints of the schedule are not met
   * within 6 hours, then the job will be dropped, without execution.
   *
   * @param time the time duration in the given <tt>unit</tt>
   * @param unit the unit of the <tt>time</tt> argument
   * @return {@link ScheduleBuilder} containing the given timeout
   */
  ScheduleBuilder setTimeout(long time, TimeUnit unit);

  /**
   * Set the max number of concurrently runs of the schedule program.
   *
   * @param max the max number of concurrently running programs allowed
   * @return {@link ConstraintProgramScheduleBuilder} containing the given max number of concurrent runs
   */
  ConstraintProgramScheduleBuilder withConcurrency(int max);

  /**
   * Set a certain amount of delay passed after the schedule is triggered, before launching the program.
   *
   * @param delay delay in the given <tt>unit</tt> to wait after the schedule is triggered before launching the program
   * @param unit the time unit of the <tt>delay</tt> argument
   * @return {@link ScheduleBuilder} containing the given delay. Note that the delay constraint does not have the
   * option to abort the schedule if the constraint is not met.
   */
  ScheduleBuilder withDelay(long delay, TimeUnit unit);

  /**
   * Set a time range in a day starting from {@code startTime} and ending at {@code endTime}, between which
   * the program is allowed to be launched. {@code endTime} must be later than {@code startTime}.
   * JVM's default time zone will be used to interpret {@code startTime} and {@code endTime}.
   *
   * @param startTime the start time (inclusive) in the format of "HH:mm", for instance 2am should be "02:00"
   * @param endTime the end time (exclusive) in the format of "HH:mm", for instance 5pm should be "17:00"
   * @return {@link ConstraintProgramScheduleBuilder} containing the time range
   */
  ConstraintProgramScheduleBuilder withTimeWindow(String startTime, String endTime);

  /**
   * Set a time range in a day starting from {@code startTime} and ending at {@code endTime} in the given time zone,
   * between which the program is allowed to be launched.
   *
   * @param startTime the start time (inclusive) in the format of "HH:mm", for instance 2am should be "02:00"
   * @param endTime the end time (exclusive) in the format of "HH:mm", for instance 5pm should be "17:00"
   * @param timeZone the time zone of {@code startTime} and {@code endTime}
   * @return {@link ConstraintProgramScheduleBuilder} containing the time range
   */
  ConstraintProgramScheduleBuilder withTimeWindow(String startTime, String endTime, TimeZone timeZone);

  /**
   * Set a certain duration passed since the last launching of the program before launching the program again.
   *
   * @param duration duration in the given <tt>unit</tt> to wait after the last launch of the program
   *                 before launching the program
   * @param unit the time unit of the <tt>duration</tt> argument
   * @return {@link ConstraintProgramScheduleBuilder} containing the given duration
   */
  ConstraintProgramScheduleBuilder withDurationSinceLastRun(long duration, TimeUnit unit);

  /**
   * Create a schedule which is triggered based upon the given cron expression.
   *
   * @param cronExpression the cron expression to specify the time to trigger the schedule
   * @return a {@link ScheduleCreationSpec}
   */
  ScheduleCreationSpec triggerByTime(String cronExpression);

  /**
   * Create a schedule which is triggered whenever at least a certain number of new partitions
   * are added to a certain dataset in the same namespace as the app.
   *
   * @param datasetName the name of the dataset in the same namespace of the app
   * @param numPartitions the minimum number of new partitions added to the dataset to trigger the schedule
   * @return a {@link ScheduleCreationSpec}
   */
  ScheduleCreationSpec triggerOnPartitions(String datasetName, int numPartitions);

  /**
   * Create a schedule which is triggered whenever at least a certain number of new partitions
   * are added to a certain dataset in the specified namespace.
   *
   * @param datasetNamespace the namespace where the dataset is defined
   * @param datasetName the name of the dataset in the specified namespace of the app
   * @param numPartitions the minimum number of new partitions added to the dataset to trigger the schedule
   * @return a {@link ScheduleCreationSpec}
   */
  ScheduleCreationSpec triggerOnPartitions(String datasetNamespace, String datasetName, int numPartitions);

  /**
   * Create a schedule which is triggered when the given program in the given namespace, application, and
   * application version transitions to any one of the given program statuses.
   *
   * @param programNamespace the namespace where this program is defined
   * @param application the name of the application where this program is defined
   * @param appVersion the version of the application
   * @param programType the type of the program, as supported by the system
   * @param program the name of the program
   * @param programStatuses the set of statuses to trigger the schedule. The schedule will be triggered if the status of
   *                        the specific program transitioned to one of these statuses.
   * @return a {@link ScheduleCreationSpec}
   */
  ScheduleCreationSpec triggerOnProgramStatus(String programNamespace, String application, String appVersion,
                                              ProgramType programType, String program,
                                              ProgramStatus... programStatuses);

  /**
   * Create a schedule which is triggered when the given program in the given namespace and application
   * transitions to any one of the given program statuses.
   *
   * @see #triggerOnProgramStatus(String, String, String, ProgramType, String, ProgramStatus...)
   */
  ScheduleCreationSpec triggerOnProgramStatus(String programNamespace, String application, ProgramType programType,
                                              String program, ProgramStatus... programStatuses);

  /**
   * Creates a schedule which is triggered when the given program given application in the same namespace
   * transitions to any one of the given program statuses.
   *
   * @see #triggerOnProgramStatus(String, String, ProgramType, String, ProgramStatus...)
   */
  ScheduleCreationSpec triggerOnProgramStatus(String application, ProgramType programType,
                                              String program, ProgramStatus... programStatuses);

  /**
   * Creates a schedule which is triggered when the given program in the same namespace, application,
   * and application version transitions to any one of the given program statuses.
   *
   * @see #triggerOnProgramStatus(String, String, ProgramType, String, ProgramStatus...)
   */
  ScheduleCreationSpec triggerOnProgramStatus(ProgramType programType, String program,
                                              ProgramStatus... programStatuses);
}
