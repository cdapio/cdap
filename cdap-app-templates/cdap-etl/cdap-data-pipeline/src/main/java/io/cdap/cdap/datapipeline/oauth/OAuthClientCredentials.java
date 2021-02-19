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
 * OAuth client credentials POJO.
 */
public class OAuthClientCredentials {
  private final String clientId;
  private final String clientSecret;

  public OAuthClientCredentials(String clientId, String clientSecret) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link OAuthClientCredentials}.
   */
  public static class Builder {
    private String clientId;
    private String clientSecret;

    public Builder() {}

    public Builder withClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder withClientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public OAuthClientCredentials build() {
      Preconditions.checkNotNull(clientId, "Client ID missing");
      Preconditions.checkNotNull(clientSecret, "Client secret missing");
      return new OAuthClientCredentials(clientId, clientSecret);
    }
  }
}
