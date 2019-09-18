/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.api.plugin;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Thrown when a {@link PluginConfig} cannot be created from provided {@link PluginProperties}.
 * This can happen if properties required by the PluginConfig are not in the PluginProperties, or when
 * the property type specified by the PluginConfig is incompatible with the value provided in the PluginProperties.
 */
public class InvalidPluginConfigException extends RuntimeException {
  private Set<String> missingProperties;
  private Set<InvalidPluginProperty> invalidProperties;

  public InvalidPluginConfigException(String message, Throwable cause) {
    super(message, cause);
    this.missingProperties = Collections.emptySet();
    this.invalidProperties = Collections.emptySet();
  }

  public InvalidPluginConfigException(String message, Set<String> missingProperties,
                                      Set<InvalidPluginProperty> invalidProperties) {
    super(message);
    this.missingProperties = Collections.unmodifiableSet(new HashSet<>(missingProperties));
    this.invalidProperties = Collections.unmodifiableSet(new HashSet<>(invalidProperties));
  }

  /**
   * @return the set of required properties that were not provided in the PluginProperties.
   */
  public Set<String> getMissingProperties() {
    return missingProperties;
  }

  /**
   * @return the set of invalid properties, usually because the value given in the PluginProperties did not match
   *         the expected type.
   */
  public Set<InvalidPluginProperty> getInvalidProperties() {
    return invalidProperties;
  }
}
