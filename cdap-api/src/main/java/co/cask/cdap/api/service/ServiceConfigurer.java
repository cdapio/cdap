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

import co.cask.cdap.api.service.http.HttpServiceHandler;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for configurers used to create custom Services.
 */
public interface ServiceConfigurer {

  /**
   * Set name of the service.
   * @param name of the service.
   */
  void setName(String name);

  /**
   * Set description of the Service.
   * @param description to set for the Service.
   */
  void setDescription(String description);

  /**
   * Add a worker to the Service.
   * @param worker to add as worker.
   */
  <T extends ServiceWorker> void addWorker(T worker);

  /**
   * Add a list of workers to the Service.
   * @param worker
   */
  <T extends ServiceWorker> void addWorkers(Iterable<T> worker);

  /**
   * Add a request handler to the Service.
   * @param handler to serve requests.
   */
  <T extends HttpServiceHandler> void addHandler(T handler);

  /**
   * Add a a list of request handlers to the Service.
   * @param handlers to serve requests.
   */
  <T extends HttpServiceHandler> void addHandlers(Iterable<T> handlers);

  /**
   * Set the properties for the Service.
   * @param properties
   */
  void setProperties(Map<String, String> properties);

  /**
   * Get the primary handler used to service requests.
   * @return the handler that serves requests.
   */
  List<HttpServiceHandler> getHandlers();

  /**
   * Get a list of workers for the Service.
   * @return a list of workers for the Service.
   */
  List<ServiceWorker> getWorkers();

  /**
   * Get the name of the Service.
   * @return name of the service.
   */
  String getName();

  /**
   * Get the description of the Service.
   * @return description of the Service.
   */
  String getDescription();

  /**
   * Get the properties for the Service.
   * @return properties of the Service.
   */
  Map<String, String> getProperties();

  /**
   * Specify a dataset to be used by the Service.
   * @param dataset name of dataset used.
   */
  void useDataset(String dataset);

  /**
   * Specify a list of datasets that will be used by the Service.
   * @param datasets names of datasets used.
   */
  void useDatasets(Iterable<String> datasets);

  /**
   * Get a set of datasets used by the Service.
   * @return set of datasets.
   */
  Set<String> getDatasets();
}
