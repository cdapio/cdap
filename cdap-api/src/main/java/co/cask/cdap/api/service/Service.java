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

/**
 * Defines a custom user Service. Services are custom applications that run in program containers and provide
 * endpoints to serve requests.
 */
public interface Service {

  /**
   * Configure the Service by adding {@link HttpServiceHandler}s to handle requests.
   * @param configurer to use to add handlers and workers to the Service.
   */
  void configure(ServiceConfigurer configurer);
}
