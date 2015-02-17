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

package co.cask.cdap.api.worker;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.dataset.Dataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Extend this class to add workers.
 */
public abstract class AbstractWorker implements Worker {

  private WorkerConfigurer configurer;
  private WorkerContext context;

  @Override
  public final void configure(WorkerConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * Set description of the {@link Worker}.
   * @param description the description
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets the resources requirements for the {@link Worker}.
   * @param resources the requirements
   */
  protected void setResources(Resources resources) {
    configurer.setResources(resources);
  }

  /**
   * Sets the number of instances needed for the {@link Worker}.
   * @param instances number of instances, must be > 0
   */
  protected void setInstances(int instances) {
    configurer.setInstances(instances);
  }

  /**
   * Sets a set of properties that will be available through the {@link WorkerSpecification#getProperties()}.
   * @param properties the properties to set
   */
  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Adds the names of {@link Dataset Datasets} used by the worker.
   * @param dataset dataset name
   * @param datasets more dataset names
   */
  protected void useDatasets(String dataset, String...datasets) {
    List<String> datasetList = new ArrayList<String>();
    datasetList.add(dataset);
    datasetList.addAll(Arrays.asList(datasets));
    useDatasets(datasetList);
  }

  /**
   * Adds the names of {@link Dataset Datasets} used by the worker.
   * @param datasets dataset names
   */
  protected void useDatasets(Iterable<String> datasets) {
    configurer.useDatasets(datasets);
  }

  /**
   * Returns the {@link WorkerConfigurer} used for configuration. Only available during configuration time.
   */
  protected final WorkerConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Configures the worker.
   */
  protected void configure() {

  }

  @Override
  public void initialize(WorkerContext context) throws Exception {
    this.context = context;
  }

  protected WorkerContext getContext() {
    return context;
  }

  @Override
  public void stop() {
    // default no-op
  }

  @Override
  public void destroy() {
    // default no-op
  }
}
