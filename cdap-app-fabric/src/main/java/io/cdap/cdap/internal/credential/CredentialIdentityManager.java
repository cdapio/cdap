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

package io.cdap.cdap.internal.credential;

import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * Manager for {@link io.cdap.cdap.proto.credential.CredentialIdentity} resources.
 */
public interface CredentialIdentityManager {

  /**
   * Lists credential identities in the given namespace.
   * @param namespace The namespace to list from.
   * @return A collection of identity references.
   * @throws IOException If experiencing failure fetching from the underlying storage.
   */
  Collection<CredentialIdentityId> list(String namespace) throws IOException;

  /**
   * Fetches a credential identity.
   *
   * @param id The credential identity to fetch.
   * @return An optional encapsulating the credential identity.
   * @throws BadRequestException If the reference is in an invalid format.
   * @throws IOException If experiencing failure fetching from the underlying storage.
   */
  Optional<CredentialIdentity> get(CredentialIdentityId id) throws BadRequestException, IOException;

  /**
   * Creates a credential identity.
   *
   * @param id The reference to the identity.
   * @param identity The identity to create.
   * @throws AlreadyExistsException If the identity already exists.
   * @throws BadRequestException If the reference is in an invalid format or the profile referenced
   *                             in the identity does not exist.
   * @throws IOException If experiencing failure writing to the underlying storage.
   */
  void create(CredentialIdentityId id, CredentialIdentity identity)
      throws AlreadyExistsException, BadRequestException, IOException;

  /**
   * Updates an existing credential identity.
   *
   * @param id The reference to the identity.
   * @param identity The updated identity.
   * @throws BadRequestException If the reference is in an invalid format or the profile referenced
   *    *                        in the identity does not exist.
   * @throws IOException If experiencing failure writing to the underlying storage.
   * @throws NotFoundException If the identity does not exist.
   */
  void update(CredentialIdentityId id, CredentialIdentity identity)
      throws BadRequestException, IOException, NotFoundException;

  /**
   * Deletes a credential identity.
   *
   * @param id The reference to the identity.
   * @throws BadRequestException If the reference is in an invalid format.
   * @throws IOException If experiencing failure writing to the underlying storage.
   * @throws NotFoundException If the identity does not exist.
   */
  void delete(CredentialIdentityId id) throws BadRequestException, IOException, NotFoundException;
}
