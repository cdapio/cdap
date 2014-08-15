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

package co.cask.cdap.internal.service;

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.http.HttpServiceHandler;

import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link co.cask.cdap.api.service.ServiceSpecification}.
 */
public class DefaultServiceSpecification implements ServiceSpecification {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;
  private final List<? extends ServiceWorker> workers;
  private final List<? extends HttpServiceHandler> serviceHandlers;

  /**
   * Create a ServiceSpecification for a custom user Service.
   * @param className of service.
   * @param name of service.
   * @param description of service.
   * @param properties of service.
   * @param workers of service.
   * @param serviceHandler of service.
   */
  public DefaultServiceSpecification(String className, String name, String description, Map<String,
                                      String> properties, List<? extends ServiceWorker> workers,
                                      List<? extends HttpServiceHandler> serviceHandler) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = properties;
    this.workers = workers;
    this.serviceHandlers = serviceHandler;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public List<? extends ServiceWorker> getWorkers() {
    return workers;
  }

  @Override
  public List<? extends HttpServiceHandler> getHandlers() {
    return serviceHandlers;
  }
}
