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

package io.cdap.cdap.internal.credential.store;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.NamespaceResourceReference;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.security.credential.CredentialIdentity;
import io.cdap.cdap.api.security.credential.CredentialIdentityMetadata;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Inject;

/**
 * Synchronized storage for {@link CredentialIdentity}.
 */
public class CredentialIdentityStore
    extends SecureMultiStore<CredentialIdentityMetadata, CredentialIdentity> {

  @Inject
  public CredentialIdentityStore(SecureStore secureStore, SecureStoreManager secureStoreManager,
      TransactionRunner transactionRunner) {
    super(secureStore, secureStoreManager, transactionRunner);
  }

  @Override
  String getResourceType() {
    return "credentialidentity";
  }

  @Override
  CredentialIdentity constructFromSecureData(CredentialIdentityMetadata metadata,
      String secureData) {
    return new CredentialIdentity(metadata, secureData);
  }

  @Override
  Optional<VersionedObject<CredentialIdentityMetadata>> getMetadataInternal(
      StructuredTableContext context, NamespaceResourceReference ref) throws IOException {
    StructuredTable identityTable =
        context.getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            ref.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.IDENTITY_NAME_FIELD,
            ref.getName()));
    Optional<StructuredRow> row = identityTable.read(key);
    if (!row.isPresent()) {
      return Optional.empty();
    }
    CredentialIdentityMetadata metadata = new CredentialIdentityMetadata(
        Util.toNamespaceResourceReference(row.get()
            .getString(StoreDefinition.CredentialProvisionerStore.IDENTITY_PROFILE_INDEX_FIELD)),
        row.get().getString(StoreDefinition.CredentialProvisionerStore.IDENTITY_VALUE_FIELD));
    return Optional.of(
        new VersionedObject<CredentialIdentityMetadata>(metadata,
            row.get().getString(StoreDefinition.CredentialProvisionerStore.SECURE_STORE_VERSION)));
  }

  @Override
  Collection<NamespaceResourceReference> listMetadataInternal(StructuredTableContext context,
      String namespace) throws IOException {
    StructuredTable identityTable =
        context.getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
    Field<?> namespaceIndex =
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            namespace);
    try (CloseableIterator<StructuredRow> iterator = identityTable.scan(Range.singleton(
        Collections.singleton(namespaceIndex)), Integer.MAX_VALUE)) {
      return identitiesFromRowIterator(iterator);
    }
  }

  @Override
  void writeMetadataInternal(StructuredTableContext context, NamespaceResourceReference ref,
      CredentialIdentityMetadata metadata, String version)
      throws IOException, IllegalArgumentException, IllegalStateException {
    // Validate the referenced profile exists.
    StructuredTable profileTable = context
        .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
    Collection<Field<?>> profileKey = new ArrayList<>();
    NamespaceResourceReference profileRef = metadata.getCredentialProvisionerProfile();
    profileKey.add(Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
        profileRef.getNamespace()));
    profileKey.add(Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_NAME_FIELD,
        profileRef.getName()));
    if (!profileTable.read(profileKey).isPresent()) {
      throw new IllegalArgumentException(String.format("Credential provisioner profile '%s:%s' "
          + "not found", profileRef.getNamespace(), profileRef.getName()));
    }
    StructuredTable identityTable =
        context.getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> row = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            ref.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.IDENTITY_NAME_FIELD,
            ref.getName()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.IDENTITY_VALUE_FIELD,
            metadata.getValue()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.IDENTITY_PROFILE_INDEX_FIELD,
            Util.toNamespaceResourceReferenceIndex(metadata.getCredentialProvisionerProfile())),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.SECURE_STORE_VERSION,
            version));
    identityTable.upsert(row);
  }

  @Override
  void deleteMetadataInternal(StructuredTableContext context, NamespaceResourceReference ref)
      throws IOException {
    StructuredTable identityTable =
        context.getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            ref.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.IDENTITY_NAME_FIELD,
            ref.getName()));
    identityTable.delete(key);
  }

  static Collection<NamespaceResourceReference> identitiesFromRowIterator(
      Iterator<StructuredRow> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
        Spliterator.ORDERED),false)
        .map(row -> new NamespaceResourceReference(
            row.getString(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD),
            row.getString(StoreDefinition.CredentialProvisionerStore.IDENTITY_NAME_FIELD)))
        .collect(Collectors.toList());
  }
}
