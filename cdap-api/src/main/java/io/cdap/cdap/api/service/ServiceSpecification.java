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

package io.cdap.cdap.api.service;

import io.cdap.cdap.api.AbstractProgramSpecification;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Specification for a {@link Service}.
 */
public final class ServiceSpecification extends AbstractProgramSpecification {
  private final Map<String, HttpServiceHandlerSpecification> handlers;
  private final Resources resources;
  private final Map<String, String> properties;
  private final int instances;

  public ServiceSpecification(String className, String name, String description,
                              Map<String, HttpServiceHandlerSpecification> handlers,
                              Resources resources, int instances, Map<String, Plugin> plugins) {
    this(className, name, description, handlers, resources, instances, plugins, Collections.emptyMap());
  }

  public ServiceSpecification(String className, String name, String description,
                              Map<String, HttpServiceHandlerSpecification> handlers,
                              Resources resources, int instances, Map<String, Plugin> plugins,
                              Map<String, String> properties) {
    super(className, name, description, plugins);
    this.handlers = Collections.unmodifiableMap(new HashMap<>(handlers));
    this.resources = resources;
    this.instances = instances;
    this.properties = properties;
  }

  /**
   * Returns an immutable map from handler name to handler specification.
   */
  public Map<String, HttpServiceHandlerSpecification> getHandlers() {
    return handlers;
  }

  /**
   * Returns the number of instances for the service handler.
   */
  public int getInstances() {
    return instances;
  }

  /**
   * Returns the resources requirements for the service handler.
   */
  public Resources getResources() {
    return resources;
  }

  /**
   * @return the properties
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @param key for getting specific property value
   *
   * @return the property value
   */
  public String getProperty(String key) {
    return properties.get(key);
  }
}
