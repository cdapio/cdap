/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.api.workflow;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.FeatureFlagsProvider;
import io.cdap.cdap.api.Predicate;
import io.cdap.cdap.api.ProgramConfigurer;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.customaction.CustomAction;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.plugin.PluginConfigurer;

/**
 * Configurer for configuring the {@link Workflow}.
 */
public interface WorkflowConfigurer
  extends ProgramConfigurer, PluginConfigurer, DatasetConfigurer, FeatureFlagsProvider {

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
  void addAction(CustomAction action);

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

  /**
   * Adds a condition to the {@link Workflow}.
   * @param condition the {@link Condition} to be evaluated
   * @return the configurer for the condition
   */
  WorkflowConditionConfigurer<? extends WorkflowConfigurer> condition(Condition condition);

  /**
   * Adds a local dataset instance to the {@link Workflow}.
   * <p>
   * Local datasets are created at the start of every {@code Workflow} run and deleted once the run
   * is complete. User can decide to keep the local datasets even after the run is complete by specifying
   * the runtime arguments - <code>dataset.dataset_name.keep.local=true</code>.
   *
   * @param datasetName name of the dataset instance
   * @param typeName name of the dataset type
   * @param properties dataset instance properties
   */
  @Beta
  void createLocalDataset(String datasetName, String typeName, DatasetProperties properties);

  /**
   * Adds a local dataset instance to the {@link Workflow}. Also deploys the dataset type
   * represented by the datasetClass parameter in the current namespace.
   * <p>
   * Local datasets are created at the start of every {@code Workflow} run and deleted once the run
   * is complete. User can decide to keep the local datasets even after the run is complete by specifying
   * the runtime arguments - <code>dataset.dataset_name.keep.local=true</code>.
   *
   * @param datasetName dataset instance name
   * @param datasetClass dataset class to create the Dataset type from
   * @param props dataset instance properties
   */
  @Beta
  void createLocalDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props);
}
