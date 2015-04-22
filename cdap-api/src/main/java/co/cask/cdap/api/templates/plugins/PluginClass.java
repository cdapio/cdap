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

package co.cask.cdap.api.templates.plugins;

import co.cask.cdap.api.annotation.Beta;

import java.util.Map;
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

  public PluginClass(String type, String name, String description, String className,
                     @Nullable String configfieldName, Map<String, PluginPropertyField> properties) {
    if (type == null) {
      throw new IllegalArgumentException("Plugin class type cannot be null");
    }
    if (name == null) {
      throw new IllegalArgumentException("Plugin class name cannot be null");
    }
    if (description == null) {
      throw new IllegalArgumentException("Plugin class description cannot be null");
    }
    if (className == null) {
      throw new IllegalArgumentException("Plugin class className cannot be null");
    }
    if (properties == null) {
      throw new IllegalArgumentException("Plugin class properties cannot be null");
    }

    this.type = type;
    this.name = name;
    this.description = description;
    this.className = className;
    this.configFieldName = configfieldName;
    this.properties = properties;
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
   * Returns a map from config property name to {@link PluginPropertyField} that are supported by the plugin class.
   */
  public Map<String, PluginPropertyField> getProperties() {
    return properties;
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

    return type.equals(that.type)
      && name.equals(that.name)
      && description.equals(that.description)
      && className.equals(that.className)
      && !(configFieldName != null ? !configFieldName.equals(that.configFieldName) : that.configFieldName != null)
      && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + name.hashCode();
    result = 31 * result + description.hashCode();
    result = 31 * result + className.hashCode();
    result = 31 * result + (configFieldName != null ? configFieldName.hashCode() : 0);
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "PluginClass{" +
      "className='" + className + '\'' +
      ", type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", configFieldName='" + configFieldName + '\'' +
      ", properties=" + properties +
      '}';
  }
}
