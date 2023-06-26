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

package io.cdap.cdap.proto.credential;

import io.cdap.cdap.proto.id.CredentialProfileId;

/**
 * Defines an identity for credential provisioning.
 */
public class CredentialIdentity {

  private final CredentialProfileId credentialProfile;
  private final String identity;
  private final String secureValue;

  /**
   * Constructs an identity.
   *
   * @param credentialProfile The associated profile.
   * @param identity The identity.
   * @param secureValue The secure value to store for the identity.
   */
  public CredentialIdentity(CredentialProfileId credentialProfile, String identity,
      String secureValue) {
    this.credentialProfile = credentialProfile;
    this.identity = identity;
    this.secureValue = secureValue;
  }

  public CredentialProfileId getCredentialProfile() {
    return credentialProfile;
  }

  public String getIdentity() {
    return identity;
  }

  public String getSecureValue() {
    return secureValue;
  }
}

