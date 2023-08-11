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
import io.cdap.cdap.security.spi.credential.SecurityTokenServiceRequest.TokenType;

/**
 * Represents a Security Token Service Token Exchange response. For more details,
 * <a href="https://cloud.google.com/iam/docs/reference/sts/rest/v1/TopLevel/token#response-body">
 *   see
 * </a>
 */
public class SecurityTokenServiceResponse {

  @SerializedName("access_token")
  private final String accessToken;

  @SerializedName("issued_token_type")
  private final TokenType issuedTokenType;

  @SerializedName("token_type")
  private final String tokenType;

  @SerializedName("expires_in")
  private final int expiresIn;

  public String getAccessToken() {
    return accessToken;
  }

  /**
   * Constructs a {@link SecurityTokenServiceResponse}.
   */
  public SecurityTokenServiceResponse(String accessToken, TokenType issuedTokenType,
      String tokenType, int expiresIn) {
    this.accessToken = accessToken;
    this.issuedTokenType = issuedTokenType;
    this.tokenType = tokenType;
    this.expiresIn = expiresIn;
  }
}
