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

package io.cdap.cdap.security.credential;

import io.cdap.cdap.api.NamespaceResourceReference;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.security.credential.CredentialProvisionerProfile;
import io.cdap.cdap.common.CredentialProfileAlreadyExistsException;
import io.cdap.cdap.common.CredentialProfileNotFoundException;

/**
 * Provides CRUD methods for {@link CredentialProvisionerProfile}.
 */
public interface CredentialProvisionerProfileStore {

  /**
   * Get a credential provisioner profile.
   * @param ref The reference to the profile
   * @throws CredentialProfileNotFoundException if the profile does not exist
   * @throws AccessException if unauthorized to retrieve the profile
   */
  CredentialProvisionerProfile get(NamespaceResourceReference ref)
      throws CredentialProfileNotFoundException, AccessException;

  /**
   * Create a credential provisioner profile.
   * @param ref The reference to the profile
   * @param profile The profile to create
   * @throws CredentialProfileAlreadyExistsException if the profile already exists
   * @throws AccessException if unauthorized to create the profile
   */
  void create(NamespaceResourceReference ref, CredentialProvisionerProfile profile)
      throws CredentialProfileAlreadyExistsException, AccessException;

  /**
   * Update a credential provisioner profile.
   * @param ref The reference to the profile
   * @param profile The updated profile
   * @throws CredentialProfileNotFoundException if the profile does not exist
   * @throws AccessException if unauthorized to update the profile
   */
  void update(NamespaceResourceReference ref, CredentialProvisionerProfile profile)
      throws CredentialProfileNotFoundException, AccessException;

  /**
   * Delete a credential provisioner profile.
   * @param ref The reference to the profile
   * @throws CredentialProfileNotFoundException if the profile does not exist
   * @throws AccessException if unauthorized to delete the profile
   */
  void delete(NamespaceResourceReference ref)
      throws CredentialProfileNotFoundException, AccessException;
}
