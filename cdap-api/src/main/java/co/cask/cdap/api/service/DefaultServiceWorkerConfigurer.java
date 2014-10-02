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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Default implementation of {@link co.cask.cdap.api.service.ServiceWorkerConfigurer}
 */
public class DefaultServiceWorkerConfigurer implements ServiceWorkerConfigurer {
  private String name;
  private String description;
  private Resources resources;
  private Map<String, String> properties;
  private ServiceWorker serviceWorker;

  public DefaultServiceWorkerConfigurer(ServiceWorker serviceWorker) {
    this.serviceWorker = serviceWorker;
    this.name = serviceWorker.getClass().getSimpleName();
    this.description = "";
    this.resources = new Resources();
    this.properties = ImmutableMap.of();
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setResources(Resources resources) {
    Preconditions.checkArgument(resources != null, "resources cannot be null.");
    this.resources = resources;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public ServiceWorkerSpecification createServiceWorkerSpec() {
    return new DefaultServiceWorkerSpecification(serviceWorker, name, description,
                                                 properties, resources);
  }
}
