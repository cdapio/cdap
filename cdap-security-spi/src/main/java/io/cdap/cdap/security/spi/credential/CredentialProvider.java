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

import io.cdap.cdap.api.security.credential.CredentialIdentity;
import io.cdap.cdap.api.security.credential.CredentialProvisioningException;
import io.cdap.cdap.api.security.credential.ProvisionedCredential;
import io.cdap.cdap.proto.credential.CredentialProfile;

/**
 * Defines an SPI for provisioning a credential.
 */
public interface CredentialProvider {

  /**
   * Returns the name of the credential provider.
   *
   * @return the name of the credential provider.
   */
  String getName();

  /**
   * Initializes the credential provider. This is guaranteed to be called once before any other
   * methods (except for {@link CredentialProvider#getName()} are called.
   *
   * @param context The credential provider context to initialize with.
   */
  void initialize(CredentialProviderContext context);

  /**
   * Provisions a short-lived credential for the provided identity using the provided credential
   * profile.
   *
   * @param profile  The credential profile to use.
   * @param identity The credential identity to use.
   * @return A credential provisioned using the specified profile and identity.
   * @throws CredentialProvisioningException If the credential provisioning fails.
   */
  ProvisionedCredential provision(CredentialProfile profile, CredentialIdentity identity)
      throws CredentialProvisioningException;

  /**
   * Validates a credential profile for this specific extension. Implementations of this function
   * should only validate fields specific to the profile type statically and should not be used to
   * provision a credential.
   *
   * @param profile The profile to validate.
   * @throws ProfileValidationException If validation fails.
   */
  void validateProfile(CredentialProfile profile) throws ProfileValidationException;
}
