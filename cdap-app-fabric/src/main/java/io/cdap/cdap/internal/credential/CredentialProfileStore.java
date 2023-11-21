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
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.security.encryption.AeadCipher;
import io.cdap.cdap.security.encryption.guice.DataStorageAeadEncryptionModule;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition.CredentialProviderStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * Storage for credential identities.
 */
public class CredentialProfileStore {

  /**
   * DO NOT CHANGE THIS VALUE! CHANGING THIS VALUE IS BACKWARDS-INCOMPATIBLE. Values encrypted using
   * a different value will not decrypt properly!
   */
  private static final byte[] CREDENTIAL_PROFILE_STORE_AD = "CredentialProviderStore"
      .getBytes(StandardCharsets.UTF_8);
  private static final Gson GSON = new Gson();

  private final AeadCipher dataStorageCipher;

  @Inject
  CredentialProfileStore(@Named(DataStorageAeadEncryptionModule.DATA_STORAGE_ENCRYPTION)
      AeadCipher dataStorageCipher) {
    this.dataStorageCipher = dataStorageCipher;
  }

  /**
   * Lists entries in the credential profile table for a given namespace.
   *
   * @param context   The transaction context to use.
   * @param namespace The namespace to list profiles from.
   * @return A collection of profiles in the namespace.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Collection<CredentialProfileId> list(StructuredTableContext context, String namespace)
      throws IOException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_PROFILES);
    Field<?> namespaceIndex = Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
        namespace);
    try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(
        Collections.singleton(namespaceIndex)), Integer.MAX_VALUE)) {
      return profilesFromRowIterator(iterator);
    }
  }

  /**
   * Returns whether an entry exists in the profile table.
   *
   * @param context The transaction context to use.
   * @param id      The profile reference to fetch.
   * @return Whether the credential profile exists.
   */
  public boolean exists(StructuredTableContext context, CredentialProfileId id) throws IOException {
    return readProfile(context, id).isPresent();
  }

  /**
   * Fetch an entry from the profile table.
   *
   * @param context The transaction context to use.
   * @param id      The profile reference to fetch.
   * @return The fetched credential profile.
   * @throws IOException If any failure reading from storage occurs.
   */
  public Optional<CredentialProfile> get(StructuredTableContext context, CredentialProfileId id)
      throws CipherException, IOException {
    return readProfile(context, id)
        .map(row -> row.getBytes(CredentialProviderStore.PROFILE_DATA_FIELD))
        .map(encryptedData -> dataStorageCipher.decrypt(encryptedData, CREDENTIAL_PROFILE_STORE_AD))
        .map(decrypted -> new String(decrypted, StandardCharsets.UTF_8))
        .map(decryptedStr -> GSON.fromJson(decryptedStr, CredentialProfile.class));
  }

  /**
   * Write an entry to the credential profile table.
   *
   * @param context The transaction context to use.
   * @param id      The profile reference to write to.
   * @param profile The profile to write.
   * @throws IOException If any failure reading from storage occurs.
   */
  public void write(StructuredTableContext context, CredentialProfileId id,
      CredentialProfile profile) throws CipherException, IOException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_PROFILES);
    Collection<Field<?>> row = Arrays.asList(
        Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProviderStore.PROFILE_NAME_FIELD,
            id.getName()),
        Fields.bytesField(CredentialProviderStore.PROFILE_DATA_FIELD,
            dataStorageCipher.encrypt(GSON.toJson(profile).getBytes(StandardCharsets.UTF_8),
                CREDENTIAL_PROFILE_STORE_AD)));
    table.upsert(row);
  }

  /**
   * Deletes an entry from the credential profile table.
   *
   * @param context The transaction context to use.
   * @param id      The profile reference to delete.
   * @throws IOException If any failure reading from storage occurs.
   */
  public void delete(StructuredTableContext context, CredentialProfileId id)
      throws IOException, NotFoundException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_PROFILES);
    Collection<Field<?>> key = Arrays.asList(
        Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProviderStore.PROFILE_NAME_FIELD,
            id.getName()));
    table.delete(key);
  }

  /**
   * Returns the count of all credential profiles.
   *
   * @param context The transaction context to use.
   * @return The number of credential profiles.
   * @throws IOException If any failure reading from storage occurs.
   */
  public long getProfileCount(StructuredTableContext context) throws IOException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_PROFILES);
    return table.count(Collections.singletonList(Range.all()));
  }

  private static Collection<CredentialProfileId> profilesFromRowIterator(
      Iterator<StructuredRow> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
        Spliterator.ORDERED), false)
        .map(row -> new CredentialProfileId(
            row.getString(CredentialProviderStore.NAMESPACE_FIELD),
            row.getString(CredentialProviderStore.PROFILE_NAME_FIELD)))
        .collect(Collectors.toList());
  }

  private Optional<StructuredRow> readProfile(StructuredTableContext context,
      CredentialProfileId id) throws IOException {
    StructuredTable table = context.getTable(CredentialProviderStore.CREDENTIAL_PROFILES);
    Collection<Field<?>> key = Arrays.asList(
        Fields.stringField(CredentialProviderStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProviderStore.PROFILE_NAME_FIELD,
            id.getName()));
    return table.read(key);
  }
}
