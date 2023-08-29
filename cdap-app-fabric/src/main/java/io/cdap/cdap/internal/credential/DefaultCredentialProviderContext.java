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

package io.cdap.cdap.internal.credential;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.CredentialProvider;
import io.cdap.cdap.common.conf.Constants.Namespace;
import io.cdap.cdap.security.spi.credential.CredentialProviderContext;
import java.util.Collections;
import java.util.Map;

/**
 * Default context used for credential provider initialization.
 */
public class DefaultCredentialProviderContext implements CredentialProviderContext {

  private final Map<String, String> properties;
  private final boolean isNamespaceCreationHookEnabled;

  /**
   * Creates a new context.
   *
   * @param cConf        The CConfiguration backing the context properties.
   * @param providerName The credential provider name.
   */
  protected DefaultCredentialProviderContext(CConfiguration cConf, String providerName) {
    String prefix = String.format("%s%s.", CredentialProvider.SYSTEM_PROPERTY_PREFIX,
        providerName);
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
    this.isNamespaceCreationHookEnabled = cConf.getBoolean(
        Namespace.NAMESPACE_CREATION_HOOK_ENABLED, false);
  }

  /**
   * Returns the properties for the provider.
   *
   * @return The properties for the provider.
   */
  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Returns a boolean if namespace creation hook is enabled.
   *
   * @return true if namespace creation hook is enabled, otherwise false.
   */
  @Override
  public boolean isNamespaceCreationHookEnabled() {
    return isNamespaceCreationHookEnabled;
  }
}
