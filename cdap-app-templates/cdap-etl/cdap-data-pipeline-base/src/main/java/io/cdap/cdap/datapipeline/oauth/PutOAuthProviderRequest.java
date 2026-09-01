/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.oauth;

import javax.annotation.Nullable;

/**
 * OAuth REST PUT request body.
 */
public class PutOAuthProviderRequest {
  private final String loginURL;
  private final String tokenRefreshURL;
  @Nullable
  private final String clientId;
  @Nullable
  private final String clientSecret;
  @Nullable
  private final OAuthProvider.CredentialEncodingStrategy strategy;
  @Nullable
  private final String userAgent;
  @Nullable
  private final AuthType authType;
  @Nullable
  private final RefreshType refreshType;

  private PutOAuthProviderRequest(String loginURL,
                                  String tokenRefreshURL,
                                  @Nullable String clientId,
                                  @Nullable String clientSecret,
                                  @Nullable OAuthProvider.CredentialEncodingStrategy strategy,
                                  @Nullable String userAgent,
                                  @Nullable AuthType authType,
                                  @Nullable RefreshType refreshType) {
    this.loginURL = loginURL;
    this.tokenRefreshURL = tokenRefreshURL;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.strategy = strategy;
    this.userAgent = userAgent;
    this.authType = authType != null ? authType : AuthType.STANDARD;
    this.refreshType = refreshType != null ? refreshType : RefreshType.STANDARD;
  }

  public String getLoginURL() {
    return loginURL;
  }

  public String getTokenRefreshURL() {
    return tokenRefreshURL;
  }

  @Nullable
  public String getClientId() {
    return clientId;
  }

  @Nullable
  public String getClientSecret() {
    return clientSecret;
  }

  @Nullable
  public OAuthProvider.CredentialEncodingStrategy getCredentialEncodingStrategy() {
    return strategy;
  }

  @Nullable
  public String getUserAgent() {
    return userAgent;
  }

  public AuthType getAuthType() {
    if (authType != null) {
      return authType;
    }
    return AuthType.STANDARD;
  }

  public RefreshType getRefreshType() {
    if (refreshType != null) {
      return refreshType;
    }
    return RefreshType.STANDARD;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link PutOAuthProviderRequest}.
   */
  public static class Builder {
    private String loginURL;
    private String tokenRefreshURL;
    private String clientId;
    private String clientSecret;
    private OAuthProvider.CredentialEncodingStrategy strategy;
    private String userAgent;
    private AuthType authType;
    private RefreshType refreshType;

    public Builder loginURL(String loginURL) {
      this.loginURL = loginURL;
      return this;
    }

    public Builder tokenRefreshURL(String tokenRefreshURL) {
      this.tokenRefreshURL = tokenRefreshURL;
      return this;
    }

    public Builder clientId(@Nullable String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder clientSecret(@Nullable String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public Builder strategy(@Nullable OAuthProvider.CredentialEncodingStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public Builder userAgent(@Nullable String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public Builder authType(@Nullable AuthType authType) {
      this.authType = authType;
      return this;
    }

    public Builder refreshType(@Nullable RefreshType refreshType) {
      this.refreshType = refreshType;
      return this;
    }

    public PutOAuthProviderRequest build() {
      return new PutOAuthProviderRequest(loginURL, tokenRefreshURL, clientId,
          clientSecret, strategy, userAgent, authType, refreshType);
    }
  }
}
