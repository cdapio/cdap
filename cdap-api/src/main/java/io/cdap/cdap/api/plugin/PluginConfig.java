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

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.annotation.Beta;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Base class for writing configuration class for template plugin.
 * This class can also be used inside the plugin configuration class to represent a collection of configs.
 * If it is used to represent a collection of configs:
 * When the plugin is deployed, the configs inside this class will be inspected and represented in the same way as
 * {@link PluginPropertyField}.
 * The {@link PluginPropertyField} for this field will contain a collection of property names about configs inside
 * this class.
 */
@Beta
public abstract class PluginConfig extends Config implements Serializable {

  private static final long serialVersionUID = 125560021489909246L;

  // below fields are set using reflection
  private PluginProperties properties;
  private PluginProperties rawProperties;
  private Set<String> macroFields;

  protected PluginConfig() {
    this.properties = PluginProperties.builder().build();
    this.rawProperties = PluginProperties.builder().build();
    this.macroFields = Collections.emptySet();
  }

  /**
   * Returns the {@link PluginProperties}.
   *
   * All plugin properties that are macro-enabled and were configured with macro syntax present will be substituted
   * with Java's default values based on the property's type at configuration time. The default values are:
   *  - boolean: false
   *  - byte: 0
   *  - char: '\u0000'
   *  - double: 0.0d
   *  - float: 0.0f
   *  - int: 0
   *  - long: 0L
   *  - short: 0
   *  - String: null
   *
   */
  public final PluginProperties getProperties() {
    return properties;
  }

  /**
   * Contains the {@link PluginProperties} as they were before macro evaluation. For example, at configure time, if the
   * 'schema' property is a macro '${schema}', the value will be the raw string '${schema}' instead of null. This is
   * primarily useful when one plugin is passing macro enabled properties to another plugin.
   */
  public final PluginProperties getRawProperties() {
    return rawProperties;
  }

  /**
   * Returns true if property value contains a macro; false otherwise. This method should only be called at
   * configure time. At runtime this will always return false, as macro substitution will have already occurred.
   *
   * @param fieldName name of the field
   * @return whether the field contains a macro or not
   */
  public boolean containsMacro(String fieldName) {
    return macroFields.contains(fieldName);
  }
}
