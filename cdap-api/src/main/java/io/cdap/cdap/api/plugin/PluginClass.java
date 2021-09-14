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

package io.cdap.cdap.api.plugin;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.annotation.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Contains information about a plugin class.
 */
@Beta
public class PluginClass {
  private final String type;
  private final String name;
  private final String category;
  private final String description;
  private final String className;
  private final List<String> runtimeClassNames;
  private final String configFieldName;
  private final Map<String, PluginPropertyField> properties;
  private final Requirements requirements;

  // for GSON deserialization should not be made public. VisibleForTesting
  PluginClass() {
    this.type = Plugin.DEFAULT_TYPE;
    this.name = null;
    this.description = "";
    this.category = null;
    this.className = null;
    this.runtimeClassNames = null;
    this.configFieldName = null;
    this.properties = Collections.emptyMap();
    this.requirements = Requirements.EMPTY;
  }

  public static Builder builder() {
    return new Builder();
  }

  private PluginClass(String type, String name, @Nullable String category, String className,
                      @Nullable List<String> runtimeClassNames, String configFieldName, Map<String,
                      PluginPropertyField> properties,
                      Requirements requirements, String description) {
    this.type = type;
    this.name = name;
    this.category = category;
    this.description = description;
    this.className = className;
    this.runtimeClassNames = runtimeClassNames;
    this.configFieldName = configFieldName;
    this.properties = properties;
    this.requirements = requirements;
  }

  /**
   * @deprecated use {@link Builder} to create the object
   */
  @Deprecated
  public PluginClass(String type, String name, String className,
                     @Nullable String configfieldName, Map<String, PluginPropertyField> properties,
                     Requirements requirements, String description) {
    this(type, name, null, className, null,
         configfieldName, properties, requirements, description);
  }

  /**
   * @deprecated use {@link Builder} to create the object
   */
  @Deprecated
  public PluginClass(String type, String name, String description, String className, @Nullable String configfieldName,
                     Map<String, PluginPropertyField> properties) {
    this(type, name, className, configfieldName, properties, Requirements.EMPTY, description);
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
   * Returns the category of the plugin.
   * If a plugin does not belong to any category, {@code null} will be returned.
   */
  @Nullable
  public String getCategory() {
    return category;
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
   * @return an ordered list of runtime classes for this plugin.
   */
  public List<String> getRuntimeClassNames() {
    //If runtimeClassNames is null, main class shouls be used in runtime
    return runtimeClassNames != null ? runtimeClassNames : Collections.singletonList(className);
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
      && Objects.equals(category, that.category)
      && Objects.equals(description, that.description)
      && Objects.equals(className, that.className)
      && Objects.equals(runtimeClassNames, that.runtimeClassNames)
      && Objects.equals(configFieldName, that.configFieldName)
      && Objects.equals(properties, that.properties)
      && Objects.equals(requirements, that.requirements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, category, description, className, runtimeClassNames,
                        configFieldName, properties, requirements);
  }

  @Override
  public String toString() {
    return "PluginClass{" +
      "type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", category='" + category + '\'' +
      ", description='" + description + '\'' +
      ", className='" + className + '\'' +
      ", runtimeClassNames=" + runtimeClassNames +
      ", configFieldName='" + configFieldName + '\'' +
      ", properties=" + properties +
      ", requirements=" + requirements +
      '}';
  }

  /**
   * A builder to create {@link PluginClass} instance.
   */
  public static final class Builder {
    public static final Comparator<Map.Entry<Integer, String>> ORDER_THEN_CLASSNAME_COMPARATOR =
      Map.Entry.<Integer, String>comparingByKey().thenComparing(Map.Entry.comparingByValue());
    private String type;
    private String name;
    private String category;
    private String description;
    private String className;
    private List<String> runtimeClassNames;
    private String configFieldName;
    private Map<String, PluginPropertyField> properties;
    private Requirements requirements;

    private Builder() {
      this.properties = new HashMap<>();
      this.runtimeClassNames = new ArrayList<>();
      this.requirements = Requirements.EMPTY;
    }

    /**
     * Set the plugin type
     */
    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    /**
     * Set the plugin name
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the plugin category
     */
    public Builder setCategory(String category) {
      this.category = category;
      return this;
    }

    /**
     * Set the plugin description
     */
    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    /**
     * Set the plugin class name
     */
    public Builder setClassName(String className) {
      this.className = className;
      return this;
    }

    /**
     * Add a classname to consider when instantiating plugin with specified order
     */
    public Builder setRuntimeClassNames(Collection<String> runtimeClassNames) {
      this.runtimeClassNames = new ArrayList<>(runtimeClassNames);
      return this;
    }

    /**
     * Set the plugin config field
     */
    public Builder setConfigFieldName(String configFieldName) {
      this.configFieldName = configFieldName;
      return this;
    }

    /**
     * Set the plugin requirements
     */
    public Builder setRequirements(Requirements requirements) {
      this.requirements = requirements;
      return this;
    }

    /**
     * Set and replace the plugin properties
     */
    public Builder setProperties(Map<String, PluginPropertyField> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
    }

    /**
     * Adds multiple properties.
     *
     * @param properties map of properties to add.
     * @return this builder
     */
    public Builder addAll(Map<String, PluginPropertyField> properties) {
      this.properties.putAll(properties);
      return this;
    }

    /**
     * Adds a property
     *
     * @param key the name of the property
     * @param value the value of the property
     * @return this builder
     */
    public Builder add(String key, PluginPropertyField value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Creates a new instance of {@link PluginClass}.
     */
    public PluginClass build() {
      PluginClass pluginClass = new PluginClass(type, name, category, className, runtimeClassNames, configFieldName,
                                                properties, requirements, description);
      pluginClass.validate();
      return pluginClass;
    }
  }
}
