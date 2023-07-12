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

import com.google.gson.Gson;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.security.spi.credential.ProfileValidationException;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.credential.store.CredentialIdentityStore;
import io.cdap.cdap.internal.credential.store.CredentialProfileStore;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.security.spi.credential.CredentialProvider;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Manages {@link CredentialProfile} resources.
 */
public class CredentialProfileManager {

  private static final Gson GSON = new Gson();

  private final CredentialIdentityStore identityStore;
  private final CredentialProfileStore profileStore;
  private final TransactionRunner transactionRunner;
  private final Map<String, CredentialProvider> credentialProviders;

  @Inject
  CredentialProfileManager(CredentialIdentityStore identityStore,
      CredentialProfileStore profileStore, TransactionRunner transactionRunner,
      CredentialProviderProvider credentialProviderProvider) {
    this.identityStore = identityStore;
    this.profileStore = profileStore;
    this.transactionRunner = transactionRunner;
    this.credentialProviders = credentialProviderProvider.loadCredentialProviders();
  }

  /**
   * Lists credential profiles in a namespace.
   *
   * @param namespace The namespace to list profiles from.
   * @return A collection of profiles in the namespace.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Collection<CredentialProfileId> list(String namespace) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return profileStore.list(context, namespace);
    }, IOException.class);
  }

  /**
   * Fetch a credential profile.
   *
   * @param id The profile reference to fetch.
   * @return The fetched credential profile.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Optional<CredentialProfile> get(CredentialProfileId id) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return profileStore.get(context, id);
    }, IOException.class);
  }

  /**
   * Creates a credential profile.
   *
   * @param id The profile reference to create.
   * @param profile The profile to create.
   * @throws BadRequestException If the profile is invalid.
   * @throws AlreadyExistsException If the profile already exists.
   * @throws IOException If any failure writing to storage occurs.
   */
  public void create(CredentialProfileId id, CredentialProfile profile)
      throws AlreadyExistsException, BadRequestException, IOException {
    validateProfile(profile);
    TransactionRunners.run(transactionRunner, context -> {
      if (profileStore.get(context, id).isPresent()) {
        throw new AlreadyExistsException(String.format("Credential profile '%s:%s' already exists",
            id.getNamespace(), id.getName()));
      }
      profileStore.write(context, id, profile);
    }, AlreadyExistsException.class, IOException.class);
  }

  /**
   * Updates a credential profile.
   *
   * @param id The profile reference to update.
   * @param profile The updated profile.
   * @throws BadRequestException If the profile is invalid.
   * @throws IOException If any failure writing to storage occurs.
   * @throws NotFoundException If the profile does not exist.
   */
  public void update(CredentialProfileId id, CredentialProfile profile)
      throws BadRequestException, IOException, NotFoundException {
    validateProfile(profile);
    TransactionRunners.run(transactionRunner, context -> {
      if (!profileStore.get(context, id).isPresent()) {
        throw new NotFoundException(String.format("Credential profile '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      profileStore.write(context, id, profile);
    }, IOException.class, NotFoundException.class);
  }

  /**
   * Deletes a credential profile.
   *
   * @param id The profile reference to delete.
   * @throws ConflictException If the profile still has attached identities.
   * @throws IOException If any failure writing to storage occurs.
   * @throws NotFoundException If the profile does not exist.
   */
  public void delete(CredentialProfileId id)
      throws ConflictException, IOException, NotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      if (!profileStore.get(context, id).isPresent()) {
        throw new NotFoundException(String.format("Credential profile '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      // Check if any existing identities are using this profile.
      Collection<CredentialIdentityId> profileIdentities = identityStore
          .listForProfile(context, id);
      if (!profileIdentities.isEmpty()) {
        throw new ConflictException(String.format("Failed to delete profile; profile still "
            + "has the following attached identities: %s", GSON.toJson(profileIdentities)));
      }
      profileStore.delete(context, id);
    }, ConflictException.class, IOException.class, NotFoundException.class);
  }

  private void validateProfile(CredentialProfile profile) throws BadRequestException {
    // Ensure provider type is valid.
    String providerType = profile.getCredentialProviderType();
    if (providerType == null || profile.getCredentialProviderType().isEmpty()) {
      throw new BadRequestException("Credential provider type cannot be null or empty.");
    }
    if (!credentialProviders.containsKey(providerType)) {
      throw new BadRequestException(String.format("Credential provider type '%s' is unsupported.",
          providerType));
    }
    try {
      credentialProviders.get(providerType).validateProfile(profile);
    } catch (ProfileValidationException e) {
      throw new BadRequestException(String.format("Failed to validate profile with error: %s",
          e.getMessage()), e);
    }
  }
}
