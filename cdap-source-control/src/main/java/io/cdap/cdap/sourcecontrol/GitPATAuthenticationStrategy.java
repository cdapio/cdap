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
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.nio.charset.StandardCharsets;

/**
 * An {@link AuthenticationStrategy} to use with GitHub and Personal Access Tokens.
 */
public class GitPATAuthenticationStrategy implements AuthenticationStrategy {
  private static final String GITHUB_PAT_USERNAME = "oauth2";

  @Override
  public CredentialsProvider getCredentialsProvider(SecureStore store,
                                                    RepositoryConfig config,
                                                    String namespaceId) throws AuthenticationConfigException {
    try {
      byte[] bytes = store.get(namespaceId, config.getAuth().getTokenName()).get();
      String token = new String(bytes, StandardCharsets.UTF_8);
      return new UsernamePasswordCredentialsProvider(GITHUB_PAT_USERNAME, token);
    } catch (Exception e) {
      throw new AuthenticationConfigException("Failed to get auth token from secure store", e);
    }
  }
}
