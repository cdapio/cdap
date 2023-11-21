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

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants.Metrics.Credential;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Manages {@link CredentialIdentity} resources.
 */
public class CredentialIdentityManager {

  private final CredentialIdentityStore identityStore;
  private final CredentialProfileStore profileStore;
  private final TransactionRunner transactionRunner;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  CredentialIdentityManager(CredentialIdentityStore identityStore,
      CredentialProfileStore profileStore, TransactionRunner transactionRunner,
      MetricsCollectionService metricsCollectionService) {
    this.identityStore = identityStore;
    this.profileStore = profileStore;
    this.transactionRunner = transactionRunner;
    this.metricsCollectionService = metricsCollectionService;
  }

  /**
   * Lists credential identities in a namespace.
   *
   * @param namespace The namespace to list identities from.
   * @return A collection of identities in the namespace.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Collection<CredentialIdentityId> list(String namespace) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return identityStore.list(context, namespace);
    }, IOException.class);
  }

  /**
   * Fetch a credential identity.
   *
   * @param id The identity reference to fetch.
   * @return The fetched credential identity.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Optional<CredentialIdentity> get(CredentialIdentityId id) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      try {
        return identityStore.get(context, id);
      } catch (CipherException e) {
        throw new IOException("Failed to decrypt identity", e);
      }
    }, IOException.class);
  }

  /**
   * Creates a credential identity.
   *
   * @param id       The identity reference to create.
   * @param identity The identity to create.
   * @throws AlreadyExistsException If the identity already exists.
   * @throws IOException            If any failure writing to storage occurs.
   * @throws NotFoundException      If the profile the identity is attached to does not exist.
   */
  public void create(CredentialIdentityId id, CredentialIdentity identity)
      throws AlreadyExistsException, IOException, NotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      if (identityStore.exists(context, id)) {
        throw new AlreadyExistsException(String.format("Credential identity '%s:%s' already exists",
            id.getNamespace(), id.getName()));
      }
      validateAndWriteIdentity(context, id, identity);
      emitCredentialIdentityCountMetric(context);
    }, AlreadyExistsException.class, IOException.class, NotFoundException.class);
  }

  /**
   * Updates a credential identity.
   *
   * @param id       The identity reference to update.
   * @param identity The identity to update.
   * @throws IOException       If any failure writing to storage occurs.
   * @throws NotFoundException If the identity does not exist or if the profile the identity is
   *                           attached to does not exist.
   */
  public void update(CredentialIdentityId id, CredentialIdentity identity)
      throws IOException, NotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      if (!identityStore.exists(context, id)) {
        throw new NotFoundException(String.format("Credential identity '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      validateAndWriteIdentity(context, id, identity);
    }, IOException.class, NotFoundException.class);
  }

  /**
   * Deletes a credential identity.
   *
   * @param id The identity reference to update.
   * @throws IOException       If any failure writing to storage occurs.
   * @throws NotFoundException If the identity does not exist.
   */
  public void delete(CredentialIdentityId id) throws IOException, NotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      if (!identityStore.exists(context, id)) {
        throw new NotFoundException(String.format("Credential identity '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      identityStore.delete(context, id);
      emitCredentialIdentityCountMetric(context);
    }, IOException.class, NotFoundException.class);
  }

  private void validateAndWriteIdentity(StructuredTableContext context, CredentialIdentityId id,
      CredentialIdentity identity) throws IOException, NotFoundException {
    // Validate the referenced profile exists.
    CredentialProfileId profileId = new CredentialProfileId(identity.getProfileNamespace(),
        identity.getProfileName());
    if (!profileStore.exists(context, profileId)) {
      throw new NotFoundException(String.format("Credential profile '%s:%s' not found",
          profileId.getNamespace(), profileId.getName()));
    }
    try {
      identityStore.write(context, id, identity);
    } catch (CipherException e) {
      throw new IOException("Failed to encrypt identity", e);
    }
  }

  private void emitCredentialIdentityCountMetric(StructuredTableContext context)
      throws IOException {
    metricsCollectionService.getContext(Collections.emptyMap())
        .gauge(Credential.CREDENTIAL_IDENTITY_COUNT, identityStore.getIdentityCount(context));
  }
}
