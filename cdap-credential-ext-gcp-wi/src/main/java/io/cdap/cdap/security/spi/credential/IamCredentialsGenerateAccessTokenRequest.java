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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a IAM Credentials General Access Token request. For more details,
 * <a href=
 * "https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts">
 * see</a>
 */
public class IamCredentialsGenerateAccessTokenRequest {

  public static final String IAM_CREDENTIALS_GENERATE_SA_TOKEN_URL_FORMAT =
      "https://iamcredentials.googleapis.com/"
          + "v1/projects/-/serviceAccounts/%s:generateAccessToken";

  @SerializedName("scope")
  private final List<String> scope;

  /**
   * Constructs a {@link IamCredentialsGenerateAccessTokenRequest}.
   */
  public IamCredentialsGenerateAccessTokenRequest(String scope) {
    this.scope = Collections.unmodifiableList(
        Arrays.stream(scope.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .distinct()
            .collect(Collectors.toList()));
  }
}
