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
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.api.service.http.HttpServiceHandler;

import java.util.Arrays;

/**
 * An abstract implementation of {@link Service}. Users may extend this to write a {@link Service}.
 *
 * The default no-op constructor must be implemented.
 */
public abstract class AbstractService implements Service {
  private ServiceConfigurer configurer;

  @Override
  public final void configure(ServiceConfigurer serviceConfigurer) {
    this.configurer = serviceConfigurer;
    configure();
  }

  /**
   * Set the name for the Service.
   * @param name of the service.
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Set the description of the Service.
   * @param description of the service.
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * @see FlowletConfigurer#addStream(Stream)
   */
  protected final void addStream(Stream stream) {
    configurer.addStream(stream);
  }

  /**
   * @see ServiceConfigurer#addDatasetModule(String, Class)
   */
  @Beta
  protected final void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    configurer.addDatasetModule(moduleName, moduleClass);
  }

  /**
   * @see ServiceConfigurer#addDatasetType(Class)
   */
  @Beta
  protected final void addDatasetType(Class<? extends Dataset> datasetClass) {
    configurer.addDatasetType(datasetClass);
  }

  /**
   * Calls {@link ServiceConfigurer#createDataset(String, String, DatasetProperties)}, passing empty properties.
   *
   * @see ServiceConfigurer#createDataset(String, String, DatasetProperties)
   */
  @Beta
  protected final void createDataset(String datasetName, String typeName) {
    configurer.createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  /**
   * Calls {@link ServiceConfigurer#createDataset(String, String, DatasetProperties)}, passing the type name and
   * properties.
   *
   * @see ServiceConfigurer#createDataset(String, String, co.cask.cdap.api.dataset.DatasetProperties)
   */
  @Beta
  protected final void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    configurer.createDataset(datasetName, typeName, properties);
  }

  /**
   * Calls {@link ServiceConfigurer#createDataset(String, String, DatasetProperties)}, passing the dataset class
   * and properties.
   *
   * @see ServiceConfigurer#createDataset(String, Class, co.cask.cdap.api.dataset.DatasetProperties)
   */
  protected final void createDataset(String datasetName, Class<? extends Dataset> datasetClass,
                                     DatasetProperties properties) {
    configurer.createDataset(datasetName, datasetClass, properties);
  }

  /**
   * Calls {@link ServiceConfigurer#createDataset(String, Class, DatasetProperties)}, passing empty properties.
   *
   * @see ServiceConfigurer#createDataset(String, Class, DatasetProperties)
   */
  protected final void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    configurer.createDataset(datasetName, datasetClass, DatasetProperties.EMPTY);
  }

  /**
   * Add handler to the Service.
   * @param handler to serve requests with.
   */
  protected void addHandler(HttpServiceHandler handler) {
    addHandlers(Arrays.asList(handler));
  }

  /**
   * Add a list of handlers to the Service.
   * @param handlers to service requests with.
   */
  protected void addHandlers(Iterable<? extends HttpServiceHandler> handlers) {
    configurer.addHandlers(handlers);
  }

  /**
   * Sets the number of instances needed for the server that runs all {@link HttpServiceHandler}s of this Service.
   * @param instances Number of instances, must be > 0.
   */
  protected void setInstances(int instances) {
    configurer.setInstances(instances);
  }

  /**
   * Sets the resources requirements for the server that runs all {@link HttpServiceHandler}s of this Service.
   * @param resources The requirements.
   */
  protected void setResources(Resources resources) {
    configurer.setResources(resources);
  }

  /**
   * Returns the {@link ServiceConfigurer}, only available at configuration time.
   */
  protected final ServiceConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Implements this method to configure this Service.
   */
  protected abstract void configure();
}
