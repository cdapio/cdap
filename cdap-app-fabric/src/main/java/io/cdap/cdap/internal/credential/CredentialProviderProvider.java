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

import io.cdap.cdap.security.spi.credential.CredentialProvider;
import java.util.Map;

/**
 * Provides {@link io.cdap.cdap.security.spi.credential.CredentialProvider}.
 */
public interface CredentialProviderProvider {

  /**
   * Returns a map of credential providers type to credential providers.
   *
   * @return A map of credential providers type to credential providers.
   */
  Map<String, CredentialProvider> loadCredentialProviders();
}
