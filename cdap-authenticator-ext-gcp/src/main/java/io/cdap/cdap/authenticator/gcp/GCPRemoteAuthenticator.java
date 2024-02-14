/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.authenticator.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import java.io.IOException;
import java.sql.Date;
import java.time.Clock;
import javax.annotation.Nullable;

/**
 * {@link RemoteAuthenticator} Authenticator which returns Google application default credentials.
 * For additional information, see https://google.aip.dev/auth/4110.
 */
public class GCPRemoteAuthenticator implements RemoteAuthenticator {

  public static final String GCP_REMOTE_AUTHENTICATOR_NAME = "gcp-remote-authenticator";

  private final GoogleCredentials googleCredentials;
  private final Clock clock;

  @Nullable
  private AccessToken accessToken;

  @VisibleForTesting
  GCPRemoteAuthenticator(GoogleCredentials googleCredentials, Clock clock,
      @Nullable AccessToken accessToken) {
    this.googleCredentials = googleCredentials;
    this.clock = clock;
    this.accessToken = accessToken;
  }

  public GCPRemoteAuthenticator() throws IOException {
    this(GoogleCredentials.getApplicationDefault(), Clock.systemUTC(), null);
  }

  @Override
  public String getName() {
    return GCP_REMOTE_AUTHENTICATOR_NAME;
  }

  @Nullable
  @Override
  public Credential getCredentials() throws IOException {
    // If access token is expired, renew the access token.
    if (accessToken == null || accessToken.getExpirationTime().before(Date.from(clock.instant()))) {
      accessToken = googleCredentials.refreshAccessToken();
    }
    return new Credential(accessToken.getTokenValue(), Credential.CredentialType.EXTERNAL_BEARER,
        accessToken.getExpirationTime().getTime() / 1000L);
  }

  /**
   * Returns the credentials for the authentication with scopes.
   */
  @Nullable
  @Override
  public Credential getCredentials(@Nullable String scopes) throws IOException {
    if (Strings.isNullOrEmpty(scopes)) {
      return getCredentials();
    }
    AccessToken accessToken =
        GoogleCredentials.getApplicationDefault().createScoped(scopes).refreshAccessToken();
    return new Credential(accessToken.getTokenValue(), Credential.CredentialType.EXTERNAL_BEARER,
        accessToken.getExpirationTime().getTime() / 1000L);
  }
}
