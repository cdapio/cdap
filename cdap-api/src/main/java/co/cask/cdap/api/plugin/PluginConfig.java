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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Beta;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Base class for writing configuration class for template plugin.
 */
@Beta
public abstract class PluginConfig extends Config implements Serializable {

  private static final long serialVersionUID = 125560021489909246L;

  // below fields are set using reflection
  private PluginProperties properties;
  private Set<String> macroFields;

  protected PluginConfig() {
    this.properties = PluginProperties.builder().build();
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
   * Returns true if property value contains a macro; false otherwise. At runtime, properties that are macro-enabled
   * and contained macro syntax will still return "true" to indicate that a macro was present at configuration time.
   * @param fieldName name of the field
   * @return whether the field contains a macro or not
   */
  public boolean containsMacro(String fieldName) {
    return macroFields.contains(fieldName);
  }
}
