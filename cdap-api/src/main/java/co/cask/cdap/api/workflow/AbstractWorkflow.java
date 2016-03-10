/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;

import java.util.Map;

/**
 * Provides a default implementation of {@link Workflow} methods for easy extension.
 *
 * <p>
 * Example of configuring a workflow:
 *
 *  <pre>
 *    <code>
 *      {@literal @}Override
 *      public void configure() {
 *        setName("PurchaseHistoryWorkflow");
 *        setDescription("PurchaseHistoryWorkflow description");
 *        addMapReduce("PurchaseHistoryBuilder");
 *      }
 *    </code>
 *  </pre>
 *
 * See the Purchase example application.
 */
public abstract class AbstractWorkflow implements Workflow {

  private WorkflowConfigurer configurer;

  @Override
  public final void configure(WorkflowConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * Override this method to configure this {@link Workflow}.
   */
  protected abstract void configure();

  /**
   * Returns the {@link WorkflowConfigurer}, only available at configuration time.
   */
  protected final WorkflowConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Sets the name of the {@link Workflow}.
   */
  protected final void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Sets the description of the {@link Workflow}.
   */
  protected final void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets the properties that will be available through the {@link WorkflowSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  protected final void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Adds a custom action to the {@link Workflow}.
   * @param action the action to be added
   */
  protected final void addAction(WorkflowAction action) {
    configurer.addAction(action);
  }

  /**
   * Adds a MapReduce program to the {@link Workflow}.
   * @param mapReduce the name of MapReduce program to be added
   */
  protected final void addMapReduce(String mapReduce) {
    configurer.addMapReduce(mapReduce);
  }

  /**
   * Adds a Spark program to the {@link Workflow}.
   * @param spark the name of the Spark program to be added
   */
  protected final void addSpark(String spark) {
    configurer.addSpark(spark);
  }

  /**
   * Adds a fork to the {@link Workflow}.
   * @return the {@link WorkflowForkConfigurer} to configure actions in the fork
   */
  protected final WorkflowForkConfigurer<? extends WorkflowConfigurer> fork() {
    return configurer.fork();
  }

  /**
   * Adds a condition to the {@link Workflow}.
   * @param predicate the {@link Predicate} to be evaluated to determine which branch to take
   * @return the {@link WorkflowConditionConfigurer} to configure the branches in the condition
   */
  protected final WorkflowConditionConfigurer<? extends WorkflowConfigurer> condition(
    Predicate<WorkflowContext> predicate) {
    return configurer.condition(predicate);
  }

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
  protected final void createLocalDataset(String datasetName, String typeName, DatasetProperties properties) {
    configurer.createLocalDataset(datasetName, typeName, properties);
  }

  /**
   * Adds a local dataset instance with {@link DatasetProperties#EMPTY} to the {@link Workflow}.
   * <p>
   * Local datasets are created at the start of every {@code Workflow} run and deleted once the run
   * is complete. User can decide to keep the local datasets even after the run is complete by specifying
   * the runtime arguments - <code>dataset.dataset_name.keep.local=true</code>.
   *
   * @param datasetName name of the dataset instance
   * @param typeName name of the dataset type
   */
  protected final void createLocalDataset(String datasetName, String typeName) {
    createLocalDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  /**
   * Adds a local dataset instance to the {@link Workflow}. Also deploys the dataset type
   * represented by the datasetClass parameter in the current namespace.
   * <p>
   * Local datasets are created at the start of every {@code Workflow} run and deleted once the run
   * is complete. User can decide to keep the local datasets even after the run is complete by specifying
   * the runtime arguments - <code>dataset.dataset_name.keep.local=true</code>.
   *
   * @param datasetName name of the dataset instance
   * @param datasetClass dataset class to create the Dataset type from
   * @param props dataset instance properties
   */
  protected final void createLocalDataset(String datasetName, Class<? extends Dataset> datasetClass,
                                          DatasetProperties props) {
    configurer.createLocalDataset(datasetName, datasetClass, props);
  }

  /**
   * Adds a local dataset instance with {@link DatasetProperties#EMPTY} to the {@link Workflow}.
   * Also deploys the dataset type represented by the datasetClass parameter in the current namespace.
   * <p>
   * Local datasets are created at the start of every {@code Workflow} run and deleted once the run
   * is complete. User can decide to keep the local datasets even after the run is complete by specifying
   * the runtime arguments - <code>dataset.dataset_name.keep.local=true</code>.
   *
   * @param datasetName name of the dataset instance
   * @param datasetClass dataset class to create the Dataset type from
   */
  protected final void createLocalDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    createLocalDataset(datasetName, datasetClass, DatasetProperties.EMPTY);
  }
}
