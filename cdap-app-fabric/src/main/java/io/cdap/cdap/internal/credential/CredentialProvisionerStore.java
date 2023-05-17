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

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import io.cdap.cdap.api.NamespaceResourceReference;
import io.cdap.cdap.api.security.credential.CredentialProvisionerProfile;
import io.cdap.cdap.common.CredentialProfileAlreadyExistsException;
import io.cdap.cdap.common.CredentialProfileNotFoundException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Store for credential provisioner profiles and identities.
 */
public class CredentialProvisionerStore {

  private static final Gson GSON = new Gson();

  private final TransactionRunner transactionRunner;

  @Inject
  public CredentialProvisionerStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /*
   * Credential Provisioner Profiles
   */

  /**
   * Gets a credential provisioner profile from the profile store.
   *
   * @param profileRef The namespace reference to the profile
   * @return The profile
   * @throws IOException If fetching from the table fails
   */
  public Optional<CredentialProvisionerProfile> getProfile(NamespaceResourceReference profileRef)
      throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable profileTable = context
          .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
      Optional<StructuredRow> row = getProfileInternal(profileTable, profileRef);
      if (!row.isPresent()) {
        return Optional.empty();
      }
      return Optional.of(GSON.fromJson(row.get()
              .getString(StoreDefinition.CredentialProvisionerStore.PROFILE_DATA_FIELD),
          CredentialProvisionerProfile.class));
    }, IOException.class);
  }

  /**
   * Creates a profile if it does not already exist, otherwise throws an exception.
   *
   * @param profileRef The reference to the profile
   * @param profile    Credential provisioner profile information
   * @throws IOException                             If inserting into the table fails
   * @throws CredentialProfileAlreadyExistsException If the profile already exists
   */
  public void createProfile(NamespaceResourceReference profileRef,
      CredentialProvisionerProfile profile)
      throws IOException, CredentialProfileAlreadyExistsException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable profileTable = context
          .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
      Optional<StructuredRow> row = getProfileInternal(profileTable, profileRef);
      if (row.isPresent()) {
        throw new CredentialProfileAlreadyExistsException(profileRef);
      }
      writeProfileInternal(profileTable, profileRef, profile);
    }, IOException.class, CredentialProfileAlreadyExistsException.class);
  }

  /**
   * Updates an existing credential provisioner profile.
   *
   * @param profileRef The reference to the profile
   * @param profile    Credential provisioner profile information
   * @throws IOException                        If inserting into the table fails
   * @throws CredentialProfileNotFoundException If the profile does not exist
   */
  public void updateProfile(NamespaceResourceReference profileRef,
      CredentialProvisionerProfile profile)
      throws IOException, CredentialProfileNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable profileTable = context
          .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
      Optional<StructuredRow> row = getProfileInternal(profileTable, profileRef);
      if (!row.isPresent()) {
        throw new CredentialProfileNotFoundException(profileRef);
      }
      writeProfileInternal(profileTable, profileRef, profile);
    }, IOException.class, CredentialProfileNotFoundException.class);
  }

  /**
   * Deletes a credential provisioner profile.
   *
   * @param profileRef The reference to the profile
   * @throws IOException
   * @throws CredentialProfileNotFoundException
   */
  public void deleteProfile(NamespaceResourceReference profileRef)
      throws IOException, CredentialProfileNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable profileTable = context
          .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
      Optional<StructuredRow> row = getProfileInternal(profileTable, profileRef);
      if (!row.isPresent()) {
        throw new CredentialProfileNotFoundException(profileRef);
      }
      profileTable
          .delete(ImmutableList.of(
              Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
                  profileRef.getNamespace()),
              Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_ID_FIELD,
                  profileRef.getName())));
    }, IOException.class, CredentialProfileNotFoundException.class);
  }

  /*
   * Helper methods
   */

  private Optional<StructuredRow> getProfileInternal(StructuredTable profileTable,
      NamespaceResourceReference profileRef) throws IOException {
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            profileRef.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_ID_FIELD,
            profileRef.getName()));
    return profileTable.read(key);
  }

  private Optional<StructuredRow> getIdentityInternal(StructuredTable identityTable,
      NamespaceResourceReference identityRef) throws IOException {
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            identityRef.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.IDENTITY_ID_FIELD,
            identityRef.getName()));
    return identityTable.read(key);
  }

  private void writeProfileInternal(StructuredTable table, NamespaceResourceReference profileRef,
      CredentialProvisionerProfile profile) throws IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
        profileRef.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_ID_FIELD,
        profileRef.getName()));
    fields.add(Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_DATA_FIELD,
        GSON.toJson(profile)));
    table.upsert(fields);
  }
}
