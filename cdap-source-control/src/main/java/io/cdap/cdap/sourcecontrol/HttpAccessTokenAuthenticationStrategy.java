/*
 * Copyright © 2025 Cask Data, Inc.
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
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.errors.UnsupportedCredentialItem;
import org.eclipse.jgit.transport.CredentialItem;
import org.eclipse.jgit.transport.TransportHttp;
import org.eclipse.jgit.transport.URIish;

/**
 * An {@link AuthenticationStrategy} for repositories that use HTTP Bearer Token authentication.
 */
public class HttpAccessTokenAuthenticationStrategy implements AuthenticationStrategy {

  private final HttpAccessTokenCredentialsProvider credentialsProvider;

  /**
   * Constructs an HttpAccessTokenAuthenticationStrategy.
   *
   * @param secureStore The secure store to retrieve credentials from.
   * @param config The repository configuration.
   * @param namespaceId The namespace ID associated with the repository.
   */
  public HttpAccessTokenAuthenticationStrategy(SecureStore secureStore, RepositoryConfig config,
      String namespaceId) {
    PatConfig httpAccessTokenConfig = config.getAuth().getPatConfig();
    this.credentialsProvider = new HttpAccessTokenCredentialsProvider(
        secureStore,
        httpAccessTokenConfig.getPasswordName(),
        namespaceId
    );
  }

  @Override
  public RefreshableCredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  /**
   * Credentials provider that adds a Bearer token as HTTP header.
   */
  static class HttpAccessTokenCredentialsProvider extends RefreshableCredentialsProvider
      implements TransportCommandConfigurator {

    private final SecureStore secureStore;
    private final String accessTokenKeyName;
    private final String namespaceId;
    private String accessToken;

    HttpAccessTokenCredentialsProvider(SecureStore secureStore, String accessTokenKeyName,
        String namespaceId) {
      this.secureStore = secureStore;
      this.accessTokenKeyName = accessTokenKeyName;
      this.namespaceId = namespaceId;
    }

    @Override
    public void refresh() throws IOException, AuthenticationConfigException {
      byte[] data;
      try {
        data = secureStore.getData(namespaceId, accessTokenKeyName);
      } catch (Exception e) {
        throw new AuthenticationConfigException("Failed to get token from secure store", e);
      }
      if (data == null) {
        throw new AuthenticationConfigException(
            String.format("Token with key name '%s' not found in secure store",
                accessTokenKeyName));
      }
      accessToken = new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public boolean isInteractive() {
      return false;
    }

    @Override
    public boolean supports(CredentialItem... credentialItems) {
      // We don't use JGit's CredentialItems for bearer auth; return true to continue.
      return true;
    }

    @Override
    public boolean get(URIish uri, CredentialItem... credentialItems)
        throws UnsupportedCredentialItem {
      // This provider uses a TransportConfigCallback to inject the Bearer token directly
      // as an HTTP header, bypassing JGit's standard username/password mechanism.
      // This method is left empty but must return true to satisfy the API contract.
      return true;
    }

    @Override
    public void configure(TransportCommand<?, ?> command) {
      command.setTransportConfigCallback(transport -> {
        if (transport instanceof TransportHttp) {
          ((TransportHttp) transport).setAdditionalHeaders(
              Collections.singletonMap("Authorization", "Bearer " + accessToken));
        }
      });
    }
  }
}
