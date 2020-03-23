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
import java.util.Objects;
import java.util.Set;

/**
 * Thrown when a {@link PluginConfig} cannot be created from provided {@link PluginProperties}.
 * This can happen if properties required by the PluginConfig are not in the PluginProperties, or when
 * the property type specified by the PluginConfig is incompatible with the value provided in the PluginProperties.
 */
public class InvalidPluginConfigException extends RuntimeException {
  private final Set<String> missingProperties;
  private final Set<InvalidPluginProperty> invalidProperties;

  public InvalidPluginConfigException(String message, Throwable cause) {
    super(message, cause);
    this.missingProperties = Collections.emptySet();
    this.invalidProperties = Collections.emptySet();
  }

  public InvalidPluginConfigException(PluginClass pluginClass, Set<String> missingProperties,
                                      Set<InvalidPluginProperty> invalidProperties) {
    super(generateErrorMessage(pluginClass, missingProperties, invalidProperties));
    this.missingProperties = Collections.unmodifiableSet(new HashSet<>(missingProperties));
    this.invalidProperties = Collections.unmodifiableSet(new HashSet<>(invalidProperties));
    invalidProperties.stream()
      .map(InvalidPluginProperty::getCause)
      .filter(Objects::nonNull)
      .forEach(this::addSuppressed);
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

  private static String generateErrorMessage(PluginClass pluginClass, Set<String> missingProperties,
                                             Set<InvalidPluginProperty> invalidProperties) {
    String baseMessage = String.format("Unable to create config for %s %s", pluginClass.getType(),
                                       pluginClass.getName());
    if (missingProperties.isEmpty() && invalidProperties.isEmpty()) {
      return baseMessage;
    }

    StringBuilder errorMessage = new StringBuilder(baseMessage);
    if (missingProperties.size() == 1) {
      errorMessage.append(String.format(" Required property '%s' is missing.", missingProperties.iterator().next()));
    } else if (missingProperties.size() > 1) {
      errorMessage.append(String.format(" Required properties '%s' are missing.",
                                        String.join(", ", missingProperties)));
    }

    for (InvalidPluginProperty invalidProperty : invalidProperties) {
      errorMessage.append(String.format(" '%s' is invalid: %s.",
                                        invalidProperty.getName(), invalidProperty.getErrorMessage()));
    }

    return errorMessage.toString();
  }
}
