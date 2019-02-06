/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.data.security;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.SecretStore;
import co.cask.cdap.securestore.spi.secret.Decoder;
import co.cask.cdap.securestore.spi.secret.Encoder;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Default implementation of secret store to persist secrets.
 */
public class DefaultSecretStore implements SecretStore {
  private final TransactionRunner transactionRunner;

  @Inject
  public DefaultSecretStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  @Override
  public <T> T get(String namespace, String name, Decoder<T> decoder) throws SecretNotFoundException, IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.SecretStore.SECRET_STORE_TABLE);
      List<Field<?>> keyFields = ImmutableList.<Field<?>>builder()
        .addAll(getKeyFields(namespace, name))
        .build();
      Optional<StructuredRow> optionalRow = table.read(keyFields);
      if (!optionalRow.isPresent()) {
        throw new SecretNotFoundException(namespace, name);
      }
      StructuredRow row = optionalRow.get();
      return decoder.decode(row.getBytes(StoreDefinition.SecretStore.SECRET_DATA_FIELD));
    }, SecretNotFoundException.class, IOException.class);
  }

  @Override
  public <T> Collection<T> list(String namespace, Decoder<T> decoder) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.SecretStore.SECRET_STORE_TABLE);
      Collection<Field<?>> partialKey = Collections.singletonList(Fields.stringField(StoreDefinition
                                                                                       .SecretStore.NAMESPACE_FIELD,
                                                                                     namespace));
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(partialKey), Integer.MAX_VALUE)) {
        List<T> list = new ArrayList<>();
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          list.add(decoder.decode(row.getBytes(StoreDefinition.SecretStore.SECRET_DATA_FIELD)));
        }

        return Collections.unmodifiableList(list);
      }
    }, IOException.class);
  }

  @Override
  public <T> void store(String namespace, String name, Encoder<T> encoder, T data) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.SecretStore.SECRET_STORE_TABLE);
      List<Field<?>> fields = ImmutableList.<Field<?>>builder()
        .addAll(getKeyFields(namespace, name))
        .add(Fields.bytesField(StoreDefinition.SecretStore.SECRET_DATA_FIELD, encoder.encode(data)))
        .build();
      table.upsert(fields);
    }, IOException.class);
  }

  @Override
  public void delete(String namespace, String name) throws SecretNotFoundException, IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.SecretStore.SECRET_STORE_TABLE);
      List<Field<?>> keyFields = ImmutableList.<Field<?>>builder()
        .addAll(getKeyFields(namespace, name))
        .build();
      if (!table.read(keyFields).isPresent()) {
        throw new SecretNotFoundException(namespace, name);
      }
      table.delete(keyFields);
    }, SecretNotFoundException.class, IOException.class);
  }

  private Collection<Field<?>> getKeyFields(String namespace, String name) {
    return Arrays.asList(Fields.stringField(StoreDefinition.SecretStore.NAMESPACE_FIELD, namespace),
                         Fields.stringField(StoreDefinition.SecretStore.SECRET_NAME_FIELD, name));
  }
}
