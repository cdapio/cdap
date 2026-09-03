/*
 * Copyright © 2025 Cask Data, Inc.
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
 * OAuth access token, with related metadata required to retrieve a long-lived access token.
 */
public class OAuthAccessToken {
  private final String accessToken;
  private final long expiresAt;
  private final String identityUrl;

  private OAuthAccessToken(String accessToken, long expiresAt, String identityUrl) {
    this.accessToken = accessToken;
    this.expiresAt = expiresAt;
    this.identityUrl = identityUrl;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public long getExpiresAt() {
    return expiresAt;
  }

  public String getIdentityUrl() {
    return identityUrl;
  }

  public boolean isExpired(long refreshBufferMs) {
    if (expiresAt <= 0) {
      return false;
    }
    return (expiresAt - System.currentTimeMillis()) <= refreshBufferMs;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link OAuthAccessToken}.
   */
  public static class Builder {
    private String accessToken;
    private long expiresAt;
    private String identityUrl;

    public Builder() {}

    public Builder withAccessToken(String accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    public Builder withExpiresAt(long expiresAt) {
      this.expiresAt = expiresAt;
      return this;
    }

    public Builder withIdentityUrl(String identityUrl) {
      this.identityUrl = identityUrl;
      return this;
    }

    public OAuthAccessToken build() {
      Preconditions.checkNotNull(accessToken, "OAuth access token missing");
      return new OAuthAccessToken(accessToken, expiresAt, identityUrl);
    }
  }
}
