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

package co.cask.cdap.api.plugin;

/**
 * Thrown when a {@link PluginConfig} cannot be created from provided {@link PluginProperties}.
 * This can happen if properties required by the PluginConfig are not in the PluginProperties, or when
 * the property type specified by the PluginConfig is incompatible with the value provided in the PluginProperties.
 */
public class InvalidPluginConfigException extends RuntimeException {

  public InvalidPluginConfigException(String message) {
    super(message);
  }

  public InvalidPluginConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
