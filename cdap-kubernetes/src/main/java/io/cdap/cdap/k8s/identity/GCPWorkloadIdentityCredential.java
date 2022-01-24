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
 */

package io.cdap.cdap.k8s.identity;

import com.google.gson.annotations.SerializedName;

/**
 * Represents a GCP credential meant for serialization. For additional information see step 6 of
 * https://cloud.google.com/anthos/multicluster-management/fleets/workload-identity#impersonate_a_service_account
 */
public class GCPWorkloadIdentityCredential {

  /**
   * Represents the source of the credential.
   */
  public class CredentialSource {
    @SerializedName("file")
    private final String file;

    private CredentialSource(String file) {
      this.file = file;
    }
  }

  /**
   * Represents the type of credential.
   */
  public enum CredentialType {
    @SerializedName("external_account")
    EXTERNAL_ACCOUNT
  }

  /**
   * Represents the type of token.
   */
  public enum TokenType {
    @SerializedName("urn:ietf:params:oauth:token-type:jwt")
    JWT
  }

  @SerializedName("type")
  private final CredentialType type;

  @SerializedName("audience")
  private final String audience;

  @SerializedName("service_account_impersonation_url")
  private final String serviceAccountImpersonationURL;

  @SerializedName("subject_token_type")
  private final TokenType subjectTokenType;

  @SerializedName("token_url")
  private final String tokenURL;

  @SerializedName("credential_source")
  private final CredentialSource credentialSource;

  public GCPWorkloadIdentityCredential(CredentialType type, String audience, String serviceAccountImpersonationURL,
                                       TokenType subjectTokenType, String tokenURL, String credentialSourceFile) {
    this.type = type;
    this.audience = audience;
    this.serviceAccountImpersonationURL = serviceAccountImpersonationURL;
    this.subjectTokenType = subjectTokenType;
    this.tokenURL = tokenURL;
    this.credentialSource = new CredentialSource(credentialSourceFile);
  }
}
