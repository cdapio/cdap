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

import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition.CredentialProviderStore;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Storage for credential identities.
 */
public class CredentialIdentityStore {

  private static final Gson GSON = new Gson();

  /**
   * Lists entries in the credential identity table for a given namespace.
   *
   * @param context The transaction context to use.
   * @param namespace The namespace to list identities from.
   * @return A collection of identities in the namespace.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Collection<CredentialIdentityId> list(StructuredTableContext context, String namespace)
      throws IOException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_IDENTITIES);
    Field<?> namespaceIndex = Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
        namespace);
    try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(
        Collections.singleton(namespaceIndex)), Integer.MAX_VALUE)) {
      return identitiesFromRowIterator(iterator);
    }
  }

  /**
   * Lists entries in the credential identity table for a given profile.
   *
   * @param context The transaction context to use.
   * @param profileId The profile to list identities for.
   * @return A Collection of identities attached to the provided profile.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Collection<CredentialIdentityId> listForProfile(StructuredTableContext context,
      CredentialProfileId profileId) throws IOException {
    Field<?> indexKey = Fields.stringField(
        CredentialProviderStore.IDENTITY_PROFILE_INDEX_FIELD, toProfileIndex(profileId));
    try (CloseableIterator<StructuredRow> identityIterator = context.getTable(
        CredentialProviderStore.CREDENTIAL_IDENTITIES).scan(indexKey)) {
      return identitiesFromRowIterator(identityIterator);
    }
  }

  /**
   * Fetch an entry from the identity table.
   *
   * @param context The transaction context to use.
   * @param id The identity reference to fetch.
   * @return The fetched credential identity.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Optional<CredentialIdentity> get(StructuredTableContext context, CredentialIdentityId id)
      throws IOException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> key = Arrays.asList(
        Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProviderStore.IDENTITY_NAME_FIELD,
            id.getName()));
    return table.read(key).map(row -> GSON.fromJson(row
        .getString(CredentialProviderStore.IDENTITY_DATA_FIELD), CredentialIdentity.class));
  }

  /**
   * Write an entry to the credential identity table.
   *
   * @param context The transaction context to use.
   * @param id The identity reference to write to.
   * @param identity The identity to write.
   * @throws IOException If any failure reading from storage occurs.
   */
  public void write(StructuredTableContext context, CredentialIdentityId id,
      CredentialIdentity identity) throws IOException {
    StructuredTable identityTable =
        context.getTable(CredentialProviderStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> row = Arrays.asList(
        Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProviderStore.IDENTITY_NAME_FIELD,
            id.getName()),
        Fields.stringField(CredentialProviderStore.IDENTITY_DATA_FIELD,
            GSON.toJson(identity)),
        Fields.stringField(CredentialProviderStore.IDENTITY_PROFILE_INDEX_FIELD,
            toProfileIndex(identity.getCredentialProfile())));
    identityTable.upsert(row);
  }

  /**
   * Deletes an entry from the credential identity table.
   *
   * @param context The transaction context to use.
   * @param id The identity reference to delete.
   * @throws IOException If any failure reading from storage occurs.
   */
  public void delete(StructuredTableContext context, CredentialIdentityId id)
      throws IOException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> key = Arrays.asList(
        Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProviderStore.IDENTITY_NAME_FIELD,
            id.getName()));
    table.delete(key);
  }

  private static Collection<CredentialIdentityId> identitiesFromRowIterator(
      Iterator<StructuredRow> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
        Spliterator.ORDERED), false)
        .map(row -> new CredentialIdentityId(
            row.getString(CredentialProviderStore.NAMESPACE_FIELD),
            row.getString(CredentialProviderStore.IDENTITY_NAME_FIELD)))
        .collect(Collectors.toList());
  }

  private static String toProfileIndex(CredentialProfileId profileId) {
    return String.format("%s:%s", profileId.getNamespace(), profileId.getName());
  }
}
