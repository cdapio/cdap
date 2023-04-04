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

import com.google.common.base.Throwables;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.eclipse.jgit.errors.UnsupportedCredentialItem;
import org.eclipse.jgit.transport.CredentialItem;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

/**
 * An {@link AuthenticationStrategy} to use with GitHub and Personal Access Tokens.
 */
public class GitPatAuthenticationStrategy implements AuthenticationStrategy {

  private static final String GITHUB_PAT_USERNAME = "oauth2";
  private final SecureStorePasswordProvider credentialsProvider;

  /**
   * Construct a Git PAT auth strategy.

   * @param secureStore {@link SecureStore} to fetch the secrets with.
   * @param config {@link RepositoryConfig}
   * @param namespaceId the namespaceId
   */
  public GitPatAuthenticationStrategy(SecureStore secureStore, RepositoryConfig config,
      String namespaceId) {
    this.credentialsProvider =
        new SecureStorePasswordProvider(secureStore, GITHUB_PAT_USERNAME,
            config.getAuth().getPatConfig().getPasswordName(), namespaceId);
  }

  @Override
  public RefreshableCredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  /**
   * A class that wraps a {@link UsernamePasswordCredentialsProvider} but fetches password from a
   * {@link SecureStore}.
   */
  private static class SecureStorePasswordProvider extends RefreshableCredentialsProvider {

    private final SecureStore secureStore;
    private final String username;
    private final String passwordKeyName;
    private final String namespaceId;
    private String password;

    SecureStorePasswordProvider(SecureStore secureStore, String username, String passwordKeyName,
        String namespaceId) {
      this.secureStore = secureStore;
      this.username = username;
      this.passwordKeyName = passwordKeyName;
      this.namespaceId = namespaceId;
    }

    @Override
    public void refresh() throws IOException, AuthenticationConfigException {
      byte[] data;
      try {
        data = secureStore.getData(namespaceId, passwordKeyName);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, IOException.class);
        throw new AuthenticationConfigException("Failed to get password from secure store", e);
      }
      if (data == null) {
        throw new AuthenticationConfigException(
            String.format("Password with key name %s not found in secure store",
                passwordKeyName));
      }
      password = new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public boolean isInteractive() {
      return false;
    }

    @Override
    public boolean supports(CredentialItem... credentialItems) {
      return new UsernamePasswordCredentialsProvider("", "").supports(credentialItems);
    }

    @Override
    public boolean get(URIish urIish, CredentialItem... credentialItems)
        throws UnsupportedCredentialItem {
      if (password == null) {
        throw new IllegalStateException(
            "Password not fetched from secure store. Refresh credentials before getting them.");
      }
      return new UsernamePasswordCredentialsProvider(username, password).get(urIish,
          credentialItems);
    }
  }
}
