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

import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.util.Optional;

/**
 * An {@link AuthStrategy} to use with GitHub and Personal Access Tokens.
 */
public class GitPATAuthStrategy implements AuthStrategy {

  public GitPATAuthStrategy() {
  }

  private String getToken(SourceControlContext context) throws Exception {
    byte[] bytes = context.getStore()
      .get(context.getNamespaceId().getNamespace(), context.getRepositoryConfig().getAuth().getTokenName())
      .get();
    return new String(bytes);
  }

  public CredentialsProvider getCredentialProvider(SourceControlContext context) throws AuthenticationException {
    try {
      return new UsernamePasswordCredentialsProvider("oauth2", getToken(context));
    } catch (Exception e) {
      throw new AuthenticationException("Failed to get auth token from secure store", e);
    }
  }

  public Optional<TransportConfigCallback> getTransportConfigCallback(SourceControlContext context) {
    return Optional.empty();
  }
}
