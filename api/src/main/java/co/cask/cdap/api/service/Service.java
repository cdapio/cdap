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
 * Defines a custom user Service.
 */
public interface Service {

  /**
   * Get the name of the service.
   * @return name of the service.
   */
  String getName();

  /**
   * Configure the service.
   * @param serviceConfigurer
   */
  void configure(ServiceConfigurer serviceConfigurer);

  /**
   * Get the primary request handler for the Service.
   * @return a request handler.
   */
  public HttpServiceHandler getHandler();

  /**
   * Get a list of workers for the service.
   * @return workers for the service.
   */
  public List<ServiceWorker> getWorkers();
}
