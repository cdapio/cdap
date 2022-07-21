/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.api.service;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.service.http.HttpServiceHandler;

import java.util.Map;

/**
 * Interface for configurers used to create custom Services.
 */
public interface ServiceConfigurer extends PluginConfigurer, DatasetConfigurer {

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

  /**
   * Sets a set of properties that will be available through the {@link ServiceSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  default void setProperties(Map<String, String> properties) {
    throw new UnsupportedOperationException("Setting properties on service is not supported");
  }
}
