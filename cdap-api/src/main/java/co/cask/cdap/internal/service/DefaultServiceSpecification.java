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

package co.cask.cdap.internal.service;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.api.service.http.HttpServiceSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * This class defines a specification for a {@link ServiceSpecification}.
 */
public class DefaultServiceSpecification implements ServiceSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Map<String, HttpServiceSpecification> handlers;
  private final Map<String, ServiceWorkerSpecification> workers;
  private final Resources resources;
  private final int instances;

  public DefaultServiceSpecification(String className, String name, String description,
                                     Map<String, HttpServiceSpecification> handlers,
                                     Map<String, ServiceWorkerSpecification> workers,
                                     Resources resources, int instances) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.handlers = ImmutableMap.copyOf(handlers);
    this.workers = ImmutableMap.copyOf(workers);
    this.resources = resources;
    this.instances = instances;
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
  public Map<String, HttpServiceSpecification> getHandlers() {
    return handlers;
  }

  @Override
  public Map<String, ServiceWorkerSpecification> getWorkers() {
    return workers;
  }

  @Override
  public Resources getResources() {
    return resources;
  }

  @Override
  public int getInstances() {
    return instances;
  }
}
