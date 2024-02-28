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

package io.cdap.cdap.sourcecontrol;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

/**
 * Provides an {@link AuthenticationStrategy}.
 */
public class AuthenticationStrategyProvider {

  private final String namespaceId;
  private final SecureStore secureStore;

  public AuthenticationStrategyProvider(String namespaceId, SecureStore secureStore) {
    this.namespaceId = namespaceId;
    this.secureStore = secureStore;
  }

  /**
   * Returns an {@link AuthenticationStrategy} for the given Git repository provider and auth type.
   *
   * @return an instance of {@link  AuthenticationStrategy}.
   * @throws AuthenticationStrategyNotFoundException when the corresponding
   *     {@link AuthenticationStrategy} is not found.
   */
  AuthenticationStrategy get(RepositoryConfig repoConfig) throws
      AuthenticationStrategyNotFoundException {
    if (repoConfig.getAuth().getType() == AuthType.PAT) {
      return new PatAuthenticationStrategy(secureStore, repoConfig, namespaceId);
    }
    throw new AuthenticationStrategyNotFoundException(
        String.format("No strategy found for provider %s and type %s.",
            repoConfig.getProvider(),
            repoConfig.getAuth().getType()));
  }
}
