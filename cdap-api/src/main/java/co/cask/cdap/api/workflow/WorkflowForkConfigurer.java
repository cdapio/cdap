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

/**
 * Defines an interface for the fork in the {@link Workflow}.
 * @param <T> the type of the object returned by the join method. For the outer fork
 *           created by the {@link WorkflowConfigurer}, join method returns {@link Void}.
 *           For the nested fork created by the {@link WorkflowForkConfigurer}, join method
 *           returns {@link WorkflowForkConfigurer} of the parent fork.
 */
public interface WorkflowForkConfigurer<T> {
  /**
   * Adds a MapReduce program as a next sequential step to the {@link WorkflowForkBranch}
   * @param mapReduce the name of the MapReduce program to be added
   * @return the configurer for the current fork
   */
  WorkflowForkConfigurer<T> addMapReduce(String mapReduce);

  /**
   * Adds a Spark program as a next sequential step to the {@link WorkflowForkBranch}
   * @param spark the name of the Spark program to be added
   * @return the configurer for the current fork
   */
  WorkflowForkConfigurer<T> addSpark(String spark);

  /**
   * Adds custom action a a next sequential step to the {@link WorkflowForkBranch}
   * @param action {@link WorkflowAction} to be added to the fork
   * @return the configurer for the current fork
   */
  WorkflowForkConfigurer<T> addAction(WorkflowAction action);

  /**
   * Adds a nested fork to the current fork
   * @return the configurer for the nested fork
   */
  WorkflowForkConfigurer<WorkflowForkConfigurer<T>> fork();

  /**
   * Adds a branch to the {@link WorkflowForkBranch}
   * @return the configurer for the fork
   */
  WorkflowForkConfigurer<T> also();

  /**
   * Joins the current fork
   * @return the configurer for the parent fork
   */
  T join();
}
