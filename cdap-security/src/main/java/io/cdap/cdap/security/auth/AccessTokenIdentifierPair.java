/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

/**
 * Access token identifier pair that has marshalled and unmarshalled
 * access token object
 */
public class AccessTokenIdentifierPair {
  private final String accessTokenIdentifierStr;
  private final AccessTokenIdentifier accessTokenIdentifierObj;

  public AccessTokenIdentifierPair(String accessTokenIdentifierStr, AccessTokenIdentifier accessTokenIdentifierObj) {
    this.accessTokenIdentifierObj = accessTokenIdentifierObj;
    this.accessTokenIdentifierStr = accessTokenIdentifierStr;
  }

  public String getAccessTokenIdentifierStr() {
    return accessTokenIdentifierStr;
  }

  public AccessTokenIdentifier getAccessTokenIdentifierObj() {
    return accessTokenIdentifierObj;
  }

  @Override
  public String toString() {
    return accessTokenIdentifierStr;
  }
}
