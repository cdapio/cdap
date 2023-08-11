/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.credential;

import com.google.gson.annotations.SerializedName;

/**
 * Represents a Security Token Service Token Exchange request. For more details,
 * <a href="https://cloud.google.com/iam/docs/reference/sts/rest/v1/TopLevel/token">see</a>
 */
public class SecurityTokenServiceRequest {

  public static String STS_ENDPOINT = "https://sts.googleapis.com/v1/token";

  /**
   * Represents the type of token.
   */
  public enum TokenType {
    @SerializedName("urn:ietf:params:oauth:token-type:jwt")
    JWT,
    @SerializedName("urn:ietf:params:oauth:token-type:access_token")
    ACCESS_TOKEN
  }

  /**
   * Represents the type of grant.
   */
  public enum GrantType {
    @SerializedName("urn:ietf:params:oauth:grant-type:token-exchange")
    TOKEN_EXCHANCE
  }

  @SerializedName("grantType")
  private final GrantType grantType;

  @SerializedName("audience")
  private final String audience;

  @SerializedName("scope")
  private final String scope;

  @SerializedName("subjectTokenType")
  private final TokenType subjectTokenType;

  @SerializedName("requestedTokenType")
  private final TokenType requestedTokenType;

  @SerializedName("subjectToken")
  private final String subjectToken;

  /**
   * Constructs a {@link SecurityTokenServiceRequest}.
   */
  public SecurityTokenServiceRequest(GrantType grantType, String audience, String scope,
      TokenType requestedTokenType, TokenType subjectTokenType, String subjectToken) {
    this.grantType = grantType;
    this.requestedTokenType = requestedTokenType;
    this.subjectTokenType = subjectTokenType;
    this.audience = audience;
    this.scope = scope;
    this.subjectToken = subjectToken;
  }
}
