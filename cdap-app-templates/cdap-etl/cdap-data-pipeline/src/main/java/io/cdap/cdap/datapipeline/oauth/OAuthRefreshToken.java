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
 * OAuth refresh token, with related metadata required to retrieve an access token.
 */
public class OAuthRefreshToken {
  private final String refreshToken;
  private final String redirectURI;

  public OAuthRefreshToken(String refreshToken, String redirectURI) {
    this.refreshToken = refreshToken;
    this.redirectURI = redirectURI;
  }

  public String getRefreshToken() {
    return refreshToken;
  }

  public String getRedirectURI() {
    return redirectURI;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link OAuthRefreshToken}.
   */
  public static class Builder {
    private String refreshToken;
    private String redirectURI;

    public Builder() {}

    public Builder withRefreshToken(String refreshToken) {
      this.refreshToken = refreshToken;
      return this;
    }

    public Builder withRedirectURI(String redirectURI) {
      this.redirectURI = redirectURI;
      return this;
    }

    public OAuthRefreshToken build() {
      Preconditions.checkNotNull(refreshToken, "OAuth refresh token missing");
      Preconditions.checkNotNull(redirectURI, "OAuth redirect URI missing");
      return new OAuthRefreshToken(refreshToken, redirectURI);
    }
  }
}
