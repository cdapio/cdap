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
 * Defines an interface for the fork in the {@link Workflow}
 */
public interface WorkflowForkConfigurer {
  /**
   * Adds a MapReduce program as a next sequential step to the {@link WorkflowForkSpecification}
   * @param mapReduce the name of the MapReduce program to be added
   * @return the configurer for the current fork
   */
  WorkflowForkConfigurer addMapReduce(String mapReduce);

  /**
   * Adds a Spark program as a next sequential step to the {@link WorkflowForkSpecification}
   * @param spark the name of the Spark program to be added
   * @return the configurer for the current fork
   */
  WorkflowForkConfigurer addSpark(String spark);

  /**
   * Adds custom action a a next sequential step to the {@link WorkflowForkSpecification}
   * @param action {@link WorkflowAction} to be added to the fork
   * @return the configurer for the current fork
   */
  WorkflowForkConfigurer addAction(WorkflowAction action);

  /**
   * Adds a nested fork as a next sequential step to the {@link WorkflowForkSpecification}
   * @return the configurer for the current fork
   */
  WorkflowForkConfigurer addFork();

  /**
   * Adds a branch to the {@link WorkflowForkSpecification}
   * @return the configurer for the fork
   */
  WorkflowForkConfigurer also();

  /**
   * Joins the current fork
   * @return the configurer for the parent fork
   */
  WorkflowForkConfigurer join();
}
