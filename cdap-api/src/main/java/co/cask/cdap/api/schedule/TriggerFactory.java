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

import java.util.Map;

/**
 * A factory for getting a specific type of {@link Trigger}
 */
public interface TriggerFactory {

  /**
   * Creates a trigger which is satisfied when at least one of the internal triggers are satisfied.
   * 
   * @param triggers the triggers to be evaluated as internal triggers of the returned trigger
   * @return a {@link Trigger}
   */
  Trigger or(Trigger... triggers);

  /**
   * Creates a trigger which is satisfied when all of the given triggers are satisfied.
   *
   * @param triggers the triggers to be evaluated as internal triggers of the returned trigger
   * @return a {@link Trigger}
   */
  Trigger and(Trigger... triggers);

  /**
   * Creates a trigger which is satisfied based upon the given cron expression.
   *
   * @param cronExpression the cron expression to specify the time to trigger the schedule
   * @return a {@link Trigger}
   */
  Trigger byTime(String cronExpression);

  /**
   * Creates a trigger which is satisfied whenever at least a certain number of new partitions
   * are added to a certain dataset in the same namespace as the app.
   *
   * @param datasetName the name of the dataset in the same namespace of the app
   * @param numPartitions the minimum number of new partitions added to the dataset to satisfy the trigger
   * @return a {@link Trigger}
   */
  Trigger onPartitions(String datasetName, int numPartitions);

  /**
   * Creates a trigger which is satisfied whenever at least a certain number of new partitions
   * are added to a certain dataset in the specified namespace.
   *
   * @param datasetNamespace the namespace where the dataset is defined
   * @param datasetName the name of the dataset in the specified namespace of the app
   * @param numPartitions the minimum number of new partitions added to the dataset to trigger the schedule
   * @return a {@link Trigger}
   */
  Trigger onPartitions(String datasetNamespace, String datasetName, int numPartitions);

  /**
   * Creates a trigger which is satisfied when the given program in the given namespace, application, and
   * application version transitions to any one of the given program statuses.
   *
   * @param programNamespace the namespace where this program is defined
   * @param application the name of the application where this program is defined
   * @param appVersion the version of the application
   * @param programType the type of the program, as supported by the system
   * @param program the name of the program
   * @param programStatuses the set of statuses to trigger the schedule. The schedule will be triggered if the status of
   *                        the specific program transitioned to one of these statuses.
   * @return a {@link Trigger}
   */
  Trigger onProgramStatus(String programNamespace, String application, String appVersion,
                          ProgramType programType, String program,
                          ProgramStatus... programStatuses);

  /**
   * Creates a trigger which is satisfied when the given program in the given namespace, application, and
   * application version transitions to any one of the given program statuses.
   *
   * @param programNamespace the namespace where this program is defined
   * @param application the name of the application where this program is defined
   * @param appVersion the version of the application
   * @param programType the type of the program, as supported by the system
   * @param program the name of the program
   * @param runtimeArgs the map with triggering program's runtime argument names as keys and the corresponding
   *                    runtime arguments to be overridden in the triggered program as values
   * @param programStatuses the set of statuses to trigger the schedule. The schedule will be triggered if the status of
   *                        the specific program transitioned to one of these statuses.
   * @return a {@link Trigger}
   */
  Trigger onProgramStatus(String programNamespace, String application, String appVersion,
                          ProgramType programType, String program, Map<String, String> runtimeArgs,
                          ProgramStatus... programStatuses);

  /**
   * Creates a trigger which is satisfied when the given program in the given namespace
   * and application with default version transitions to any one of the given program statuses.
   *
   * @see #onProgramStatus(String, String, String, ProgramType, String, ProgramStatus...)
   */
  Trigger onProgramStatus(String programNamespace, String application, ProgramType programType,
                          String program, ProgramStatus... programStatuses);

  /**
   * Creates a trigger which is satisfied when the given program in the given application in the same namespace
   * transitions to any one of the given program statuses.
   *
   * @see #onProgramStatus(String, String, ProgramType, String, ProgramStatus...)
   */
  Trigger onProgramStatus(String application, ProgramType programType,
                          String program, ProgramStatus... programStatuses);

  /**
   * Creates a trigger which is satisfied when the given program in the same namespace, application,
   * and application version transitions to any one of the given program statuses.
   *
   * @see #onProgramStatus(String, String, String, ProgramType, String, ProgramStatus...)
   */
  Trigger onProgramStatus(ProgramType programType, String program, ProgramStatus... programStatuses);

  /**
   * Creates a trigger which is satisfied when the given program in the same namespace, application,
   * and application version transitions to any one of the given program statuses.
   *
   * @see #onProgramStatus(String, String, String, ProgramType, String, ProgramStatus...)
   */
  Trigger onProgramStatus(ProgramType programType, String program, Map<String, String> runtimeArgs,
                          ProgramStatus... programStatuses);
}
