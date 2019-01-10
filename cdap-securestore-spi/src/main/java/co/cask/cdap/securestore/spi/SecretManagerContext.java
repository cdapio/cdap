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
 */

package co.cask.cdap.securestore.spi;

import java.util.Map;

/**
 * Secrets Manager context.
 */
public interface SecretManagerContext {

  /**
   * System properties are derived from the CDAP configuration. Anything in the CDAP configuration that is prefixed by
   * 'securestore.system.properties.[securestore-name].' will be added as an entry in the system properties.
   *
   * @return unmodifiable system properties for the secrets manager
   */
  Map<String, String> getProperties();

  /**
   * Gets the store implementation to store encrypted secrets and metadata associated with the secrets.
   * The store is a cdap owned store meaning it should not contain any sensitive information in plain text.
   *
   * @return secrets metadata store
   */
  SecretStore getSecretStore();
}
