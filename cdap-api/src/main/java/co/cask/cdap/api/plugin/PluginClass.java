/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.plugin;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.annotation.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Contains information about a plugin class.
 */
@Beta
public class PluginClass {
  private final String type;
  private final String name;
  private final String description;
  private final String className;
  private final String configFieldName;
  private final Map<String, PluginPropertyField> properties;
  private final Set<String> endpoints;
  private final Requirements requirements;

  // for GSON deserialization should not be made public. VisibleForTesting
  PluginClass() {
    this.type = Plugin.DEFAULT_TYPE;
    this.name = null;
    this.description = "";
    this.className = null;
    this.configFieldName = null;
    this.properties = Collections.emptyMap();
    this.endpoints = Collections.emptySet();
    this.requirements = Requirements.EMPTY;
  }

  public PluginClass(String type, String name, String description, String className,
                     @Nullable String configfieldName, Map<String, PluginPropertyField> properties,
                     Set<String> endpoints, Requirements requirements) {
    this.type = type;
    this.name = name;
    this.description = description;
    this.className = className;
    this.configFieldName = configfieldName;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.endpoints = Collections.unmodifiableSet(new HashSet<>(endpoints));
    this.requirements = requirements;
  }

  public PluginClass(String type, String name, String description, String className, @Nullable String configfieldName,
                     Map<String, PluginPropertyField> properties) {
    this(type, name, description, className, configfieldName, properties, Collections.emptySet(),
         Requirements.EMPTY);
  }

  public PluginClass(String type, String name, String description, String className,
                     @Nullable String configfieldName, Map<String, PluginPropertyField> properties,
                     Set<String> endpoints) {
    this(type, name, description, className, configfieldName, properties, endpoints,
         Requirements.EMPTY);
  }

  /**
   * Validates the {@link PluginClass}
   *
   * @throws IllegalArgumentException if any of the required fields are invalid
   */
  public void validate() {
    if (name == null) {
      throw new IllegalArgumentException("Plugin class name cannot be null.");
    }
    if (className == null) {
      throw new IllegalArgumentException("Plugin class className cannot be null.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Plugin class type cannot be null");
    }
    if (description == null) {
      throw new IllegalArgumentException("Plugin class description cannot be null");
    }
    if (properties == null) {
      throw new IllegalArgumentException("Plugin class properties cannot be null");
    }

    if (endpoints == null) {
      throw new IllegalArgumentException("Plugin class endpoints cannot be null");
    }
    if (requirements == null) {
      throw new IllegalArgumentException("Plugin class requirements cannot be null");
    }
  }

  /**
   * Returns the type name of the plugin.
   */
  public String getType() {
    return type;
  }

  /**
   * Returns name of the plugin.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns description of the plugin.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns the fully qualified class name of the plugin.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Returns the name of the field that extends from {@link PluginConfig} in the plugin class.
   * If no such field, {@code null} will be returned.
   */
  @Nullable
  public String getConfigFieldName() {
    return configFieldName;
  }

  /**
   * Returns the set of plugin endpoints available in the plugin.
   * If no such field will return empty set.
   */
  public Set<String> getEndpoints() {
    return endpoints;
  }

  /**
   * Returns a map from config property name to {@link PluginPropertyField} that are supported by the plugin class.
   */
  public Map<String, PluginPropertyField> getProperties() {
    return properties;
  }

  /**
   * @return the {@link Requirements} which represents the requirements of the plugin
   */
  public Requirements getRequirements() {
    return requirements;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginClass that = (PluginClass) o;

    return Objects.equals(type, that.type)
      && Objects.equals(name, that.name)
      && Objects.equals(description, that.description)
      && Objects.equals(className, that.className)
      && Objects.equals(configFieldName, that.configFieldName)
      && Objects.equals(properties, that.properties)
      && Objects.equals(endpoints, that.endpoints)
      && Objects.equals(requirements, that.requirements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, description, className, configFieldName, properties, endpoints, requirements);
  }

  @Override
  public String toString() {
    return "PluginClass{" +
      "type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", className='" + className + '\'' +
      ", configFieldName='" + configFieldName + '\'' +
      ", properties=" + properties +
      ", endpoints=" + endpoints +
      ", requirements=" + requirements +
      '}';
  }
}
