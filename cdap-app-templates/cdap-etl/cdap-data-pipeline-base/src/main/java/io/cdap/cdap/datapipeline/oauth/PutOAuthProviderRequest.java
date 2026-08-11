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

/**
 * OAuth REST PUT request body.
 */
public class PutOAuthProviderRequest {
  private final String loginURL;
  private final String tokenRefreshURL;
  private final String clientId;
  private final String clientSecret;
  private final OAuthProvider.CredentialEncodingStrategy strategy;
  private final String userAgent;
  private final OAuthProvider.AuthType authType;
  private final OAuthProvider.RefreshType refreshType;

  public PutOAuthProviderRequest(
          String loginURL,
          String tokenRefreshURL,
          String clientId,
          String clientSecret,
          OAuthProvider.CredentialEncodingStrategy strategy,
          String userAgent) {
    this(loginURL, tokenRefreshURL, clientId, clientSecret, strategy, userAgent, OAuthProvider.AuthType.STANDARD,
         OAuthProvider.RefreshType.STANDARD);
  }

  public PutOAuthProviderRequest(
          String loginURL,
          String tokenRefreshURL,
          String clientId,
          String clientSecret,
          OAuthProvider.CredentialEncodingStrategy strategy,
          String userAgent,
          OAuthProvider.AuthType authType,
          OAuthProvider.RefreshType refreshType) {
    this.loginURL = loginURL;
    this.tokenRefreshURL = tokenRefreshURL;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.strategy = strategy;
    this.userAgent = userAgent;
    this.authType = authType == null ? OAuthProvider.AuthType.STANDARD : authType;
    this.refreshType = refreshType == null ? OAuthProvider.RefreshType.STANDARD : refreshType;
  }

  public String getLoginURL() {
    return loginURL;
  }

  public String getTokenRefreshURL() {
    return tokenRefreshURL;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public OAuthProvider.CredentialEncodingStrategy getCredentialEncodingStrategy() {
    return strategy;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public OAuthProvider.AuthType getAuthType() {
    return authType == null ? OAuthProvider.AuthType.STANDARD : authType;
  }

  public OAuthProvider.RefreshType getRefreshType() {
    return refreshType == null ? OAuthProvider.RefreshType.STANDARD : refreshType;
  }
}
