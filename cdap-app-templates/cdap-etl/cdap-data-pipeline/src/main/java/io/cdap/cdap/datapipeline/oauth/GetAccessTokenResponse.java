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
 * OAuth credential REST GET response body for retrieving an access token for a credential.
 */
public class GetAccessTokenResponse {
  private final String accessToken;
  private final String instanceURL;

  public GetAccessTokenResponse(String accessToken, String instanceURL) {
    this.accessToken = accessToken;
    this.instanceURL = instanceURL;
  }
}
