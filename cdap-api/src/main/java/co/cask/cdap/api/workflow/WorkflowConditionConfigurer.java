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
   * {@link Workflow} consists of multiple {@link WorkflowNode}s.
   * Same MapReduce program can be added multiple times in the {@link Workflow} at
   * different {@link WorkflowNode}s.
   * <p>
   * This method allows associating the uniqueName to the {@link WorkflowNode}
   * which represents the MapReduce program. The uniqueName helps querying for the
   * values that were added to the {@link WorkflowToken} by the particular node.
   * <p>
   * The uniqueName must be unique across all {@link WorkflowNode} in the Workflow,
   * otherwise the Application deployment will fail.
   * @param uniqueName the uniqueName to be assigned to the {@link WorkflowNode}
   *                   which represents the MapReduce program
   * @param mapReduce  the name of the MapReduce program to be added to the {@link Workflow}
   */
  WorkflowConditionConfigurer<T> addMapReduce(String uniqueName, String mapReduce);

  /**
   * Adds a Spark program as a next sequential step to the current branch of the {@link WorkflowConditionNode}.
   * @param spark the name of the Spark program to be added
   * @return the configurer for the current condition
   */
  WorkflowConditionConfigurer<T> addSpark(String spark);

  /**
   * {@link Workflow} consists of multiple {@link WorkflowNode}s.
   * Same Spark program can be added multiple times in the {@link Workflow} at
   * different {@link WorkflowNode}s.
   * <p>
   * This method allows associating the uniqueName to the {@link WorkflowNode}
   * which represents the Spark program. The uniqueName helps querying for the
   * values that were added to the {@link WorkflowToken} by the particular node.
   * <p>
   * The uniqueName must be unique across all {@link WorkflowNode} in the Workflow,
   * otherwise the Application deployment will fail.
   * @param uniqueName the uniqueName to be assigned to the {@link WorkflowNode}
   *                   which represents the Spark program
   * @param spark      the name of the Spark program to be added to the {@link Workflow}
   */
  WorkflowConditionConfigurer<T> addSpark(String uniqueName, String spark);

  /**
   * Adds custom action a a next sequential step to the current branch of the {@link WorkflowConditionNode}.
   * @param action {@link WorkflowAction} to be added
   * @return the configurer for the current condition
   */
  WorkflowConditionConfigurer<T> addAction(WorkflowAction action);

  /**
   * {@link Workflow} consists of multiple {@link WorkflowNode}s.
   * Same Workflow action can be added multiple times in the {@link Workflow} at
   * different {@link WorkflowNode}s.
   * <p>
   * This method allows associating the uniqueName to the {@link WorkflowNode}
   * which represents the Workflow action. The uniqueName helps querying for the
   * values that were added to the {@link WorkflowToken} by the particular node.
   * <p>
   * The uniqueName must be unique across all {@link WorkflowNode} in the Workflow,
   * otherwise the Application deployment will fail.
   * @param uniqueName the uniqueName to be assigned to the {@link WorkflowNode}
   *                   which represents the Spark program
   * @param action     to be added to the {@link Workflow}
   * @return the configurer for the current condition
   */
  WorkflowConditionConfigurer<T> addAction(String uniqueName, WorkflowAction action);

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
   * Multiple conditions can be added to the {@link Workflow}.
   * <p>
   * This method allows associating the uniqueName to the {@link WorkflowNode}
   * which represents the condition. The uniqueName helps querying for the
   * values that were added to the {@link WorkflowToken} by the particular node.
   * <p>
   * The uniqueName must be unique across all {@link WorkflowNode} in the Workflow,
   * otherwise the Application deployment will fail.
   * @param uniqueName the uniqueName to be assigned to the {@link WorkflowNode}
   *                   which represents the condition
   * @param condition  the {@link Predicate} to be evaluated for the condition
   * @return the configurer for the condition
   */
  WorkflowConditionConfigurer<? extends WorkflowConditionConfigurer<T>> condition(String uniqueName,
                                                                                  Predicate<WorkflowContext> condition);

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
