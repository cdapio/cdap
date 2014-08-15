/*
 * Copyright 2014 Cask, Inc.
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

/**
 * Interface for Configurers used to create custom Services.
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
  void addWorker(ServiceWorker worker);

  /**
   * Set the primary request handler for the Service.
   * @param serviceHandler to serve requests.
   */
  void setHandler(HttpServiceHandler serviceHandler);

  /**
   * Get the primary handler used to service requests.
   * @return the handler that serves requests.
   */
  HttpServiceHandler getHandler();

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
}
