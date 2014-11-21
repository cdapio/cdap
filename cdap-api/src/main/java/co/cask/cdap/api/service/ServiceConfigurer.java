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
import co.cask.cdap.api.service.http.HttpServiceHandler;

import java.util.Map;

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
   * Add a list of workers to the Service.
   * @param workers map from worker name to {@link ServiceWorker}.
   */
  void addWorkers(Map<String, ServiceWorker> workers);

  /**
   * Add a a list of request handlers to the Service.
   * @param handlers to serve requests.
   */
  void addHandlers(Iterable<? extends HttpServiceHandler> handlers);

  /**
   * Sets the resources requirements for the server that runs all {@link HttpServiceHandler}s of this Service.
   * @param resources The requirements.
   */
  void setResources(Resources resources);

  /**
   * Sets the number of instances needed for the server that runs all {@link HttpServiceHandler}s of this Service.
   * @param instances Number of instances, must be > 0.
   */
  void setInstances(int instances);
}
