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

package co.cask.cdap.internal.service.http;

import co.cask.cdap.api.service.http.HttpServiceSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A simple implementation of {@link HttpServiceSpecification}.
 */
public class DefaultHttpServiceSpecification implements HttpServiceSpecification {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;

  /**
   * Instantiate this class with the following parameters.
   *
   * @param className
   * @param name
   * @param description
   * @param properties
   */
  public DefaultHttpServiceSpecification(String className, String name,
                                         String description, Map<String, String> properties) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = ImmutableMap.copyOf(properties);
  }

  /**
   * Instantiate this class with the following parammeters.
   *
   * @param name
   * @param description
   * @param properties
   */
  public DefaultHttpServiceSpecification(String name, String description, Map<String, String> properties) {
    this(null, name, description, properties);
  }

  /**
   * @return the class name
   */
  @Override
  public String getClassName() {
    return className;
  }

  /**
   * @return the name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @return the description
   */
  @Override
  public String getDescription() {
    return description;
  }

  /**
   * @return the properties
   */
  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @param key for getting specific property value
   * @return the property value
   */
  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }
}
