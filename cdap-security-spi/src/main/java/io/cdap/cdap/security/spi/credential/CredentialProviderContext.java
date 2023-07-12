/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.credential;

import java.util.Map;

/**
 * Context passed to {@link CredentialProvider} during initialization.
 */
public interface CredentialProviderContext {

  /**
   * System properties are derived from the CDAP configuration. Anything in the CDAP configuration
   * that is prefixed by 'credential.provider.system.properties.[provider-name].' will be adding as
   * an entry in the system properties. For example, if the provider is named 'abc', and there is a
   * configuration property 'credential.provider.system.properties.abc.retry.timeout' with value
   * '60', the system properties map will contain a key 'retry.timeout' with value '60'. System
   * properties are not visible to end users and cannot be overwritten by end users.
   *
   * @return Unmodifiable system properties for the provider.
   */
  Map<String, String> getProperties();
}
