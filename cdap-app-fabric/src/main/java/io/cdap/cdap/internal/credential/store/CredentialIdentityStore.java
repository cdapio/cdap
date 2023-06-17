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
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition.CredentialProvisionerStore;
import java.io.IOException;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage for credential identities.
 */
public class CredentialIdentityStore {

  private static final Logger LOG = LoggerFactory.getLogger(CredentialIdentityStore.class);
  private static final Gson GSON = new Gson();

  private final TransactionRunner transactionRunner;

  @Inject
  CredentialIdentityStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  public Collection<CredentialIdentityId> list(String namespace) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
      Field<?> namespaceIndex = Fields.stringField(CredentialProvisionerStore.NAMESPACE_FIELD,
          namespace);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(
          Collections.singleton(namespaceIndex)), Integer.MAX_VALUE)) {
        return identitiesFromRowIterator(iterator);
      }
    }, IOException.class);
  }

  public Optional<CredentialIdentity> get(CredentialIdentityId id) throws IOException {
    Utils.validateResourceName(id.getName());
    return TransactionRunners.run(transactionRunner, context -> {
      Optional<StructuredRow> row = fetchRow(context, id);
      if (!row.isPresent()) {
        return Optional.empty();
      }
      return Optional.of(GSON.fromJson(row.get()
          .getString(CredentialProvisionerStore.IDENTITY_DATA_FIELD), CredentialIdentity.class));
    }, IOException.class);
  }

  public void create(CredentialIdentityId id, CredentialIdentity identity)
      throws AlreadyExistsException, IOException {
    Utils.validateResourceName(id.getName());
    TransactionRunners.run(transactionRunner, context -> {
      if (fetchRow(context, id).isPresent()) {
        throw new AlreadyExistsException(String.format("Credential identity '%s:%s' already exists",
            id.getNamespace(), id.getName()));
      }
      writeIdentity(context, id, identity);
    }, AlreadyExistsException.class, IOException.class);
  }

  public void update(CredentialIdentityId id, CredentialIdentity identity)
      throws IOException, NotFoundException {
    Utils.validateResourceName(id.getName());
    TransactionRunners.run(transactionRunner, context -> {
      if (!fetchRow(context, id).isPresent()) {
        throw new NotFoundException(String.format("Credential identity '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      writeIdentity(context, id, identity);
    }, IOException.class, NotFoundException.class);
  }

  public void delete(CredentialIdentityId id) throws IOException, NotFoundException {
    Utils.validateResourceName(id.getName());
    TransactionRunners.run(transactionRunner, context -> {
      if (!fetchRow(context, id).isPresent()) {
        throw new NotFoundException(String.format("Credential identity '%s:%s' not found",
            id.getNamespace(), id.getName()));
      }
      StructuredTable table = context.getTable(CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
      Collection<Field<?>> key = Collections.unmodifiableList(Arrays.asList(
          Fields.stringField(CredentialProvisionerStore.NAMESPACE_FIELD,
              id.getNamespace()),
          Fields.stringField(CredentialProvisionerStore.IDENTITY_NAME_FIELD,
              id.getName())));
      table.delete(key);
    }, IOException.class, NotFoundException.class);
  }

  private Optional<StructuredRow> fetchRow(StructuredTableContext context,
      CredentialIdentityId id) throws IOException {
    StructuredTable table = context.getTable(CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> key = Collections.unmodifiableList(Arrays.asList(
        Fields.stringField(CredentialProvisionerStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProvisionerStore.IDENTITY_NAME_FIELD,
            id.getName())));
    return table.read(key);
  }

  private void writeIdentity(StructuredTableContext context, CredentialIdentityId id,
      CredentialIdentity identity) throws IOException {
    // Validate the referenced profile exists.
    StructuredTable profileTable = context
        .getTable(CredentialProvisionerStore.CREDENTIAL_PROFILES);
    Collection<Field<?>> profileKey = new ArrayList<>();
    CredentialProfileId profileRef = identity.getCredentialProfile();
    profileKey.add(Fields.stringField(CredentialProvisionerStore.NAMESPACE_FIELD,
        profileRef.getNamespace()));
    profileKey.add(Fields.stringField(CredentialProvisionerStore.PROFILE_NAME_FIELD,
        profileRef.getName()));
    if (!profileTable.read(profileKey).isPresent()) {
      throw new IllegalArgumentException(String.format("Credential profile '%s:%s' not found",
          profileRef.getNamespace(), profileRef.getName()));
    }
    StructuredTable identityTable =
        context.getTable(CredentialProvisionerStore.CREDENTIAL_IDENTITIES);
    Collection<Field<?>> row = Collections.unmodifiableList(Arrays.asList(
        Fields.stringField(CredentialProvisionerStore.NAMESPACE_FIELD,
            id.getNamespace()),
        Fields.stringField(CredentialProvisionerStore.IDENTITY_NAME_FIELD,
            id.getName()),
        Fields.stringField(CredentialProvisionerStore.IDENTITY_DATA_FIELD,
            GSON.toJson(identity)),
        Fields.stringField(CredentialProvisionerStore.IDENTITY_PROFILE_INDEX_FIELD,
            Utils.toProfileIndex(profileRef))));
    identityTable.upsert(row);
  }

  static Collection<CredentialIdentityId> identitiesFromRowIterator(
      Iterator<StructuredRow> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
        Spliterator.ORDERED),false)
        .map(row -> new CredentialIdentityId(
            row.getString(CredentialProvisionerStore.NAMESPACE_FIELD),
            row.getString(CredentialProvisionerStore.IDENTITY_NAME_FIELD)))
        .collect(Collectors.toList());
  }
}
