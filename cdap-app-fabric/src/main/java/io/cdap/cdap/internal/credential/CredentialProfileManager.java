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
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.id.CredentialProfileId;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * Manager for {@link CredentialProfile} resources.
 */
public interface CredentialProfileManager {

  /**
   * Lists credential profiles in the given namespace.
   * @param namespace The namespace to list from.
   * @return A collection of profile references.
   * @throws IOException If experiencing failure fetching from the underlying storage.
   */
  Collection<CredentialProfileId> list(String namespace) throws IOException;

  /**
   * Fetches a credential profile.
   *
   * @param id The credential profile to fetch.
   * @return An optional encapsulating the credential profile.
   * @throws BadRequestException If the reference is in an invalid format.
   * @throws IOException If experiencing failure fetching from the underlying storage.
   */
  Optional<CredentialProfile> get(CredentialProfileId id) throws BadRequestException, IOException;

  /**
   * Creates a credential profile.
   *
   * @param id The reference to the profile.
   * @param profile The profile to create.
   * @throws AlreadyExistsException If the profile already exists.
   * @throws BadRequestException If the reference is in an invalid format.
   * @throws IOException If experiencing failure writing to the underlying storage.
   */
  void create(CredentialProfileId id, CredentialProfile profile)
      throws AlreadyExistsException, BadRequestException, IOException;

  /**
   * Updates an existing credential profile.
   *
   * @param id The reference to the profile.
   * @param profile The updated profile.
   * @throws BadRequestException If the reference is in an invalid format.
   * @throws IOException If experiencing failure writing to the underlying storage.
   * @throws NotFoundException If the profile does not exist.
   */
  void update(CredentialProfileId id, CredentialProfile profile)
      throws BadRequestException, IOException, NotFoundException;

  /**
   * Deletes a credential profile.
   *
   * @param id The reference to the profile.
   * @throws BadRequestException If the reference is in an invalid format.
   * @throws ConflictException If the profile still has associated identities.
   * @throws IOException If experiencing failure writing to the underlying storage.
   * @throws NotFoundException If the profile does not exist.
   */
  void delete(CredentialProfileId id)
      throws BadRequestException, ConflictException, IOException, NotFoundException;
}
