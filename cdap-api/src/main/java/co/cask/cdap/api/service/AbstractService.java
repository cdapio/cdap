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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.service.http.HttpServiceHandler;

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
   * Add handler to the Service.
   * @param handler to serve requests with.
   */
  protected void addHandler(HttpServiceHandler handler) {
    configurer.addHandler(handler);
  }

  /**
   * Add a list of handlers to the Service.
   * @param handlers to service requests with.
   */
  protected void addHandlers(Iterable<? extends HttpServiceHandler> handlers) {
    configurer.addHandlers(handlers);
  }

  /**
   * Add a worker to the Service.
   * @param worker for the service.
   */
  @Beta
  protected void addWorker(ServiceWorker worker) {
    configurer.addWorker(worker);
  }

  /**
   * Add a list of workers to the Service.
   * @param workers for the service.
   */
  @Beta
  protected void addWorkers(Iterable<? extends ServiceWorker> workers) {
    configurer.addWorkers(workers);
  }

  /**
   * Specify a dataset that will be used by the Service.
   * @param dataset name of dataset.
   */
  protected void useDataset(String dataset) {
    configurer.useDataset(dataset);
  }

  /**
   * Specify a list of datasets that will be used by the Service.
   * @param datasets names of datasets.
   */
  protected void useDatasets(Iterable<String> datasets) {
    configurer.useDatasets(datasets);
  }

  /**
   * Implement this method and use a {@link ServiceConfigurer} to add a request handler
   * and workers.
   */
  protected abstract void configure();

}
