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
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.credential.store.CredentialIdentityStore;
import io.cdap.cdap.internal.credential.store.CredentialProfileStore;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.util.Collection;
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

  @Inject
  CredentialProfileManager(CredentialIdentityStore identityStore,
      CredentialProfileStore profileStore, TransactionRunner transactionRunner) {
    this.identityStore = identityStore;
    this.profileStore = profileStore;
    this.transactionRunner = transactionRunner;
  }

  public Collection<CredentialProfileId> list(String namespace) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return profileStore.list(context, namespace);
    }, IOException.class);
  }

  public Optional<CredentialProfile> get(CredentialProfileId id) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return profileStore.get(context, id);
    }, IOException.class);
  }

  public void create(CredentialProfileId id, CredentialProfile profile)
      throws AlreadyExistsException, IOException {
    TransactionRunners.run(transactionRunner, context -> {
      if (profileStore.get(context, id).isPresent()) {
        throw new AlreadyExistsException(String.format("Credential profile '%s:%s' already exists",
            id.getNamespace(), id.getName()));
      }
      profileStore.write(context, id, profile);
    }, AlreadyExistsException.class, IOException.class);
  }

  public void update(CredentialProfileId id, CredentialProfile profile)
      throws IOException, NotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      if (!profileStore.get(context, id).isPresent()) {
        throw new NotFoundException(String.format("Credential profile '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      profileStore.write(context, id, profile);
    }, IOException.class, NotFoundException.class);
  }

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
}
