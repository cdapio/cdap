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

/**
 * Contains information about a property used by a plugin.
 */
@Beta
public class PluginPropertyField {

  private final String name;
  private final String description;
  private final String type;
  private final boolean required;

  public PluginPropertyField(String name, String description, String type, boolean required) {
    if (name == null) {
      throw new IllegalArgumentException("Plugin property name cannot be null");
    }
    if (description == null) {
      throw new IllegalArgumentException("Plugin property description cannot be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("Plugin property type cannot be null");
    }

    this.name = name;
    this.description = description;
    this.type = type;
    this.required = required;
  }

  /**
   * Returns name of the property.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns description for the property.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns {@code true} if the property is required by the plugin, {@code false} otherwise.
   */
  public boolean isRequired() {
    return required;
  }

  /**
   * Returns the type of the property.
   */
  public String getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginPropertyField that = (PluginPropertyField) o;

    return required == that.required
      && name.equals(that.name)
      && description.equals(that.description)
      && type.equals(that.type);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + description.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + (required ? 1 : 0);
    return result;
  }
}
