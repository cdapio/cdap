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
import co.cask.cdap.api.customaction.CustomAction;

/**
 * Defines an interface for the conditions in the {@link Workflow}.
 * @param <T> the type of the parent configurer
 */
public interface WorkflowConditionConfigurer<T> {
  /**
   * Adds a MapReduce program as a next sequential step to the current branch of the {@link WorkflowConditionNode}.
   * @param mapReduce the name of the MapReduce program to be added
   * @return the configurer for the current condition
   */
  WorkflowConditionConfigurer<T> addMapReduce(String mapReduce);

  /**
   * Adds a Spark program as a next sequential step to the current branch of the {@link WorkflowConditionNode}.
   * @param spark the name of the Spark program to be added
   * @return the configurer for the current condition
   */
  WorkflowConditionConfigurer<T> addSpark(String spark);

  /**
   * Adds custom action a a next sequential step to the current branch of the {@link WorkflowConditionNode}.
   * @param action {@link CustomAction} to be added
   * @return the configurer for the current condition
   */
  WorkflowConditionConfigurer<T> addAction(CustomAction action);

  /**
   * Forks the current branch of the {@link WorkflowConditionNode}.
   * @return the configurer for the fork
   */
  WorkflowForkConfigurer<? extends WorkflowConditionConfigurer<T>> fork();

  /**
   * Adds a nested condition to the current condition.
   * @return the configurer for the nested condition
   */
  WorkflowConditionConfigurer<? extends WorkflowConditionConfigurer<T>> condition(Predicate<WorkflowContext> condition);

  /**
   * Adds a nested condition to the current condition.
   * @param condition the {@link Condition} to be evaluated
   * @return the configurer for the condition
   */
  WorkflowConditionConfigurer<? extends WorkflowConditionConfigurer<T>> condition(Condition condition);

  /**
   * Adds a branch to the {@link WorkflowConditionNode} which is executed if the condition evaluates to the false.
   * @return the configurer for the condition
   */
  WorkflowConditionConfigurer<T> otherwise();

  /**
   * Ends the current condition.
   * @return the configurer for the parent condition
   */
  T end();
}
