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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Default implementation of {@link ServiceWorkerSpecification}
 */
public class DefaultServiceWorkerSpecification implements ServiceWorkerSpecification {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;

  /**
   * Create a new instance of ServiceWorkerSpecification.
   * @param className of worker.
   * @param name of worker.
   * @param description of worker.
   * @param properties of worker.
   */
  public DefaultServiceWorkerSpecification(String className, String name, String description,
                                           Map<String, String> properties) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = ImmutableMap.copyOf(properties);
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
}
