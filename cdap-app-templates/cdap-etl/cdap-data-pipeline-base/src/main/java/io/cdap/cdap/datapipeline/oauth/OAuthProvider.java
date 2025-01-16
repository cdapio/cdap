/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.oauth;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;

/**
 * OAuth provider POJO.
 */
public class OAuthProvider {
  private final String name;
  private final String loginURL;
  private final String tokenRefreshURL;
  @Nullable
  private final OAuthClientCredentials clientCreds;
  private final CredentialEncodingStrategy strategy;

  // Optional string to send as a USER_AGENT header
  @Nullable
  private final String userAgent;

  public OAuthProvider(String name,
                       String loginURL,
                       String tokenRefreshURL,
                       @Nullable OAuthClientCredentials clientCreds,
                       @Nullable CredentialEncodingStrategy strategy,
                       @Nullable String userAgent) {
    this.name = name;
    this.loginURL = loginURL;
    this.tokenRefreshURL = tokenRefreshURL;
    this.clientCreds = clientCreds;
    this.strategy = strategy;
    this.userAgent = userAgent;
  }

  public String getName() {
    return name;
  }

  public String getLoginURL() {
    return loginURL;
  }

  public String getTokenRefreshURL() {
    return tokenRefreshURL;
  }

  @Nullable
  public OAuthClientCredentials getClientCredentials() {
    return clientCreds;
  }

  public CredentialEncodingStrategy getCredentialEncodingStrategy() {
    return strategy;
  }

  @Nullable
  public String getUserAgent() {
    return userAgent;
  }

  public enum CredentialEncodingStrategy {
    // (default) Sends client ID & secret as part of the POST request body
    FORM_BODY,
    // Sends client ID & secret as part of a HTTP Basic Auth header
    BASIC_AUTH,
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link OAuthProvider}.
   */
  public static class Builder {
    private String name;
    private String loginURL;
    private String tokenRefreshURL;
    private OAuthClientCredentials clientCreds;
    private CredentialEncodingStrategy strategy;
    private String userAgent;

    public Builder() {}

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withLoginURL(String loginURL) {
      this.loginURL = loginURL;
      return this;
    }

    public Builder withTokenRefreshURL(String tokenRefreshURL) {
      this.tokenRefreshURL = tokenRefreshURL;
      return this;
    }

    public Builder withClientCredentials(@Nullable OAuthClientCredentials clientCredentials) {
      this.clientCreds = clientCredentials;
      return this;
    }

    public Builder withCredentialEncodingStrategy(@Nullable CredentialEncodingStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public Builder withUserAgent(@Nullable String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public OAuthProvider build() {
      Preconditions.checkNotNull(name, "OAuth provider name missing");
      Preconditions.checkNotNull(loginURL, "Login URL missing");
      Preconditions.checkNotNull(tokenRefreshURL, "Token refresh URL missing");
      // Default to FORM_BODY strategy
      if (strategy == null) {
        this.strategy = CredentialEncodingStrategy.FORM_BODY;
      }
      return new OAuthProvider(name, loginURL, tokenRefreshURL, clientCreds, strategy, userAgent);
    }
  }
}
