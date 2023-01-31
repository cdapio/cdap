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
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import org.eclipse.jgit.transport.CredentialsProvider;

/**
 * Interface to provide authentication objects used by JGit for different
 * {@link io.cdap.cdap.proto.sourcecontrol.Provider}s and {@link io.cdap.cdap.proto.sourcecontrol.AuthType}s.
 */
public interface AuthenticationStrategy {
  /**
   * Returns a credential provider for authenticating with a remote git repository.
   *
   * @param store       a {@link SecureStore} to fetch credentials
   * @param config      Configuration for the remote repository.
   * @param namespaceId The namespace information for fetching credentials from secure store.
   * @return a Credential provider to be used with all git commands.
   * @throws AuthenticationConfigException when there are problems creating the credential provider for the provided
   *                                       config.
   */
  CredentialsProvider getCredentialsProvider(SecureStore store, RepositoryConfig config, String namespaceId) throws
    AuthenticationConfigException;
}
