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

/**
 * OAuth provider POJO.
 */
public class OAuthProvider {
  private final String name;
  private final String loginURL;
  private final String tokenRefreshURL;
  private final OAuthClientCredentials clientCreds;

  public OAuthProvider(String name, String loginURL, String tokenRefreshURL, OAuthClientCredentials clientCreds) {
    this.name = name;
    this.loginURL = loginURL;
    this.tokenRefreshURL = tokenRefreshURL;
    this.clientCreds = clientCreds;
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

  public OAuthClientCredentials getClientCredentials() {
    return clientCreds;
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

    public Builder withClientCredentials(OAuthClientCredentials clientCredentials) {
      this.clientCreds = clientCredentials;
      return this;
    }

    public OAuthProvider build() {
      Preconditions.checkNotNull(name, "OAuth provider name missing");
      Preconditions.checkNotNull(loginURL, "Login URL missing");
      Preconditions.checkNotNull(tokenRefreshURL, "Token refresh URL missing");
      Preconditions.checkNotNull(clientCreds, "OAuth client credentials missing");
      return new OAuthProvider(name, loginURL, tokenRefreshURL, clientCreds);
    }
  }
}
