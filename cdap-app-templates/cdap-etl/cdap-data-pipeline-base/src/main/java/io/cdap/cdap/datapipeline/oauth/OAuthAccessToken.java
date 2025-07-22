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

  public OAuthAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link OAuthAccessToken}.
   */
  public static class Builder {
    private String accessToken;

    public Builder() {}

    public Builder withAccessToken(String accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    public OAuthAccessToken build() {
      Preconditions.checkNotNull(accessToken, "OAuth access token missing");
      return new OAuthAccessToken(accessToken);
    }
  }
}
