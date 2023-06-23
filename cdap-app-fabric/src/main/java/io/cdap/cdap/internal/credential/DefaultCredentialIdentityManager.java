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
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.credential.store.CredentialIdentityStore;
import io.cdap.cdap.internal.credential.store.CredentialProfileStore;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Manages {@link CredentialIdentity} resources.
 */
public class DefaultCredentialIdentityManager implements CredentialIdentityManager {

  private static final Gson GSON = new Gson();

  private final CredentialIdentityStore identityStore;
  private final CredentialProfileStore profileStore;
  private final TransactionRunner transactionRunner;

  @Inject
  DefaultCredentialIdentityManager(CredentialIdentityStore identityStore,
      CredentialProfileStore profileStore, TransactionRunner transactionRunner) {
    this.identityStore = identityStore;
    this.profileStore = profileStore;
    this.transactionRunner = transactionRunner;
  }

  @Override
  public Collection<CredentialIdentityId> list(String namespace) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return identityStore.list(context, namespace);
    }, IOException.class);
  }

  @Override
  public Optional<CredentialIdentity> get(CredentialIdentityId id)
      throws BadRequestException, IOException {
    CredentialManagerUtils.validateResourceName(id.getName());
    return TransactionRunners.run(transactionRunner, context -> {
      return identityStore.get(context, id);
    }, IOException.class);
  }

  @Override
  public void create(CredentialIdentityId id, CredentialIdentity identity)
      throws AlreadyExistsException, BadRequestException, IOException {
    CredentialManagerUtils.validateResourceName(id.getName());
    TransactionRunners.run(transactionRunner, context -> {
      if (identityStore.get(context, id).isPresent()) {
        throw new AlreadyExistsException(String.format("Credential identity '%s:%s' already exists",
            id.getNamespace(), id.getName()));
      }
      validateAndWriteIdentity(context, id, identity);
    }, AlreadyExistsException.class, BadRequestException.class, IOException.class);
  }

  @Override
  public void update(CredentialIdentityId id, CredentialIdentity identity)
      throws BadRequestException, IOException, NotFoundException {
    CredentialManagerUtils.validateResourceName(id.getName());
    TransactionRunners.run(transactionRunner, context -> {
      if (!identityStore.get(context, id).isPresent()) {
        throw new NotFoundException(String.format("Credential identity '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      validateAndWriteIdentity(context, id, identity);
    }, BadRequestException.class, IOException.class, NotFoundException.class);
  }

  @Override
  public void delete(CredentialIdentityId id)
      throws BadRequestException, IOException, NotFoundException {
    CredentialManagerUtils.validateResourceName(id.getName());
    TransactionRunners.run(transactionRunner, context -> {
      if (!identityStore.get(context, id).isPresent()) {
        throw new NotFoundException(String.format("Credential identity '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      identityStore.delete(context, id);
    }, IOException.class, NotFoundException.class);
  }

  private void validateAndWriteIdentity(StructuredTableContext context, CredentialIdentityId id,
      CredentialIdentity identity) throws BadRequestException, IOException {
    // Validate the referenced profile exists.
    CredentialProfileId profileId = identity.getCredentialProfile();
    if (!profileStore.get(context, profileId).isPresent()) {
      throw new BadRequestException(String.format("Credential profile '%s:%s' not found",
          profileId.getNamespace(), profileId.getName()));
    }
    identityStore.write(context, id, identity);
  }
}
