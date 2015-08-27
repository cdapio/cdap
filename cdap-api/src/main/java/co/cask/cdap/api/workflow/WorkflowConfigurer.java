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

package co.cask.cdap.api.workflow;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.ProgramConfigurer;

/**
 * Configurer for configuring the {@link Workflow}.
 */
public interface WorkflowConfigurer extends ProgramConfigurer {

  /**
   * Adds a MapReduce program as a next sequential step in the {@link Workflow}. MapReduce program must be
   * configured when the Application is defined. Application deployment will fail if the MapReduce program does
   * not exist.
   *
   * @param mapReduce name of the MapReduce program to be added to the {@link Workflow}
   *
   */
  void addMapReduce(String mapReduce);

  /**
   * Adds a Spark program as a next sequential step in the {@link Workflow}. Spark program must be
   * configured when the Application is defined. Application deployment will fail if the Spark program
   * does not exist.
   *
   * @param spark name of the Spark program to be added to the {@link Workflow}
   *
   */
  void addSpark(String spark);

  /**
   * Adds a custom action as a next sequential step in the {@link Workflow}
   *
   * @param action to be added to the {@link Workflow}
   */
  void addAction(WorkflowAction action);

  /**
   * Forks the execution of the {@link Workflow} into multiple branches
   * @return the configurer for the {@link Workflow}
   */
  WorkflowForkConfigurer<? extends WorkflowConfigurer> fork();

  /**
   * Adds a condition to the {@link Workflow}.
   * @param condition the {@link Predicate} to be evaluated for the condition
   * @return the configurer for the condition
   */
  WorkflowConditionConfigurer<? extends WorkflowConfigurer> condition(Predicate<WorkflowContext> condition);
}
