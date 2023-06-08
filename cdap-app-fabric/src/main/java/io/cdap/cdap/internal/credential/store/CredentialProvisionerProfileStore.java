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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.NamespaceResourceReference;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.security.credential.CredentialProvisionerProfile;
import io.cdap.cdap.api.security.credential.CredentialProvisionerProfileMetadata;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronized storage for {@link CredentialProvisionerProfile}.
 */
public class CredentialProvisionerProfileStore
    extends SecureMultiStore<CredentialProvisionerProfileMetadata, CredentialProvisionerProfile> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CredentialProvisionerProfileStore.class);
  private static final Gson GSON = new Gson();

  @Inject
  public CredentialProvisionerProfileStore(SecureStore secureStore,
      SecureStoreManager secureStoreManager, TransactionRunner transactionRunner) {
    super(secureStore, secureStoreManager, transactionRunner);
  }

  @Override
  String getResourceType() {
    return "credentialprofile";
  }

  @Override
  CredentialProvisionerProfile constructFromSecureData(
      CredentialProvisionerProfileMetadata metadata, String secureData) {
    Map<String, String> secureProperties = GSON.fromJson(secureData,
        new TypeToken<Map<String, String>>(){}.getType());
    return new CredentialProvisionerProfile(metadata, secureProperties);
  }

  @Override
  Optional<VersionedObject<CredentialProvisionerProfileMetadata>> getMetadataInternal(
      StructuredTableContext context, NamespaceResourceReference ref) throws IOException {
    StructuredTable profileTable = context
        .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            ref.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_NAME_FIELD,
            ref.getName()));
    Optional<StructuredRow> row = profileTable.read(key);
    if (!row.isPresent()) {
      return Optional.empty();
    }
    CredentialProvisionerProfileMetadata metadata = GSON.fromJson(
        row.get().getString(StoreDefinition.CredentialProvisionerStore.PROFILE_DATA_FIELD),
        CredentialProvisionerProfileMetadata.class);
    return Optional.of(new VersionedObject<CredentialProvisionerProfileMetadata>(metadata,
            row.get().getString(StoreDefinition.CredentialProvisionerStore.SECURE_STORE_VERSION)));
  }

  @Override
  Collection<NamespaceResourceReference> listMetadataInternal(StructuredTableContext context,
      String namespace) throws IOException {
    StructuredTable profileTable = context
        .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
    Field<?> namespaceIndex = Fields
        .stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD, namespace);
    try (CloseableIterator<StructuredRow> iterator = profileTable.scan(Range.singleton(Collections
        .singleton(namespaceIndex)), Integer.MAX_VALUE)) {
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
          Spliterator.ORDERED),false)
          .map(row -> new NamespaceResourceReference(
              row.getString(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD),
              row.getString(StoreDefinition.CredentialProvisionerStore.PROFILE_NAME_FIELD)))
          .collect(Collectors.toList());
    }
  }

  @Override
  void writeMetadataInternal(StructuredTableContext context, NamespaceResourceReference ref,
      CredentialProvisionerProfileMetadata metadata, String version) throws IOException {
    StructuredTable profileTable = context
        .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
    Collection<Field<?>> row = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            ref.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_NAME_FIELD,
            ref.getName()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_DATA_FIELD,
            GSON.toJson(metadata)),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.SECURE_STORE_VERSION,
            version));
    profileTable.upsert(row);
  }

  @Override
  void deleteMetadataInternal(StructuredTableContext context, NamespaceResourceReference ref)
      throws IOException {
    StructuredTable profileTable = context
        .getTable(StoreDefinition.CredentialProvisionerStore.CREDENTIAL_PROVISIONER_PROFILES);
    // Check if any existing identities are using this profile.
    Field<?> indexKey = Fields.stringField(
        StoreDefinition.CredentialProvisionerStore.IDENTITY_PROFILE_INDEX_FIELD,
        Util.toNamespaceResourceReferenceIndex(ref));
    try (CloseableIterator<StructuredRow> identityIterator = context.getTable(
        StoreDefinition.CredentialProvisionerStore.CREDENTIAL_IDENTITIES).scan(indexKey)) {
      if (identityIterator.hasNext()) {
        throw new IllegalStateException(String.format("Failed to delete profile; profile still has "
                + "the following attached identities: %s",
            GSON.toJson(CredentialIdentityStore.identitiesFromRowIterator(identityIterator))));
      }
    }
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.NAMESPACE_FIELD,
            ref.getNamespace()),
        Fields.stringField(StoreDefinition.CredentialProvisionerStore.PROFILE_NAME_FIELD,
            ref.getName()));
    profileTable.delete(key);
  }
}
