/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.service;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.worker.AbstractWorker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Extend this class to add workers to a custom Service.
 *
 * @deprecated As of version 2.8.0, replaced by {@link AbstractWorker}
 */
@Deprecated
public abstract class AbstractServiceWorker implements ServiceWorker {

  private ServiceWorkerConfigurer configurer;
  private ServiceWorkerContext context;

  @Override
  public final void configure(ServiceWorkerConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * Set description of the {@link ServiceWorker}.
   * @param description the description
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets the resources requirements for the {@link ServiceWorker}.
   * @param resources The requirements.
   */
  protected void setResources(Resources resources) {
    configurer.setResources(resources);
  }

  /**
   * Sets the number of instances needed for the {@link ServiceWorker}.
   * @param instances Number of instances, must be > 0.
   */
  protected void setInstances(int instances) {
    configurer.setInstances(instances);
  }

  /**
   * Sets a set of properties that will be available through the {@link ServiceWorkerSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Adds the names of {@link co.cask.cdap.api.dataset.Dataset DataSets} used by the worker.
   *
   * @param dataset Dataset name
   * @param datasets more Dataset names.
   */
  protected void useDatasets(String dataset, String...datasets) {
    List<String> datasetList = new ArrayList<String>();
    datasetList.add(dataset);
    datasetList.addAll(Arrays.asList(datasets));
    useDatasets(datasetList);
  }

  /**
   * Adds the names of {@link co.cask.cdap.api.dataset.Dataset DataSets} used by the worker.
   *
   * @param datasets Dataset names.
   */
  protected void useDatasets(Iterable<String> datasets) {
    configurer.useDatasets(datasets);
  }

  /**
   * Returns the {@link ServiceWorkerConfigurer} used for configuration. Only available during configuration time.
   */
  protected final ServiceWorkerConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Configures the worker.
   */
  protected void configure() {

  }

  @Override
  public void initialize(ServiceWorkerContext context) throws Exception {
    this.context = context;
  }

  protected ServiceWorkerContext getContext() {
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
