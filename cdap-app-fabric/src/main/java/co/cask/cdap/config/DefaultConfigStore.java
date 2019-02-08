/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.config;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Default Configuration Store. A "configuration" consists of a namespace, type, name, and properties map
 * The primary key is namespace, type, and name.
 *
 * For example, the ConsoleSettingsStore uses this to store user-specific configurations.
 * The type is "usersettings", and name is the user name.
 * "
 */
public class DefaultConfigStore implements ConfigStore {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final TransactionRunner transactionRunner;

  @Inject
  public DefaultConfigStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void create(String namespace, String type, Config config) throws ConfigExistsException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.ConfigStore.CONFIGS);
      List<Field<?>> primaryKey = getPrimaryKey(namespace, type, config.getName());
      Optional<StructuredRow> row = table.read(primaryKey);
      if (row.isPresent()) {
        throw new ConfigExistsException(namespace, type, config.getName());
      }
      table.upsert(toFields(namespace, type, config));
    }, ConfigExistsException.class);
  }

  @Override
  public void createOrUpdate(String namespace, String type, Config config) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.ConfigStore.CONFIGS);
      table.upsert(toFields(namespace, type, config));
    });
  }

  @Override
  public void delete(String namespace, String type, String name) throws ConfigNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.ConfigStore.CONFIGS);
      List<Field<?>> primaryKey = getPrimaryKey(namespace, type, name);
      Optional<StructuredRow> row = table.read(primaryKey);
      if (!row.isPresent()) {
        throw new ConfigNotFoundException(namespace, type, name);
      }
      table.delete(primaryKey);
    }, ConfigNotFoundException.class);
  }

  @Override
  public List<Config> list(String namespace, String type) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.ConfigStore.CONFIGS);

      List<Field<?>> scanStart = new ArrayList<>(2);
      scanStart.add(Fields.stringField(StoreDefinition.ConfigStore.NAMESPACE_FIELD, namespace));
      scanStart.add(Fields.stringField(StoreDefinition.ConfigStore.TYPE_FIELD, type));
      Range range = Range.singleton(scanStart);
      try (CloseableIterator<StructuredRow> iter = table.scan(range, Integer.MAX_VALUE)) {
        List<Config> result = new ArrayList<>();
        while (iter.hasNext()) {
          StructuredRow row = iter.next();
          result.add(fromRow(row));
        }
        return result;
      }
    });
  }

  @Override
  public Config get(String namespace, String type, String name) throws ConfigNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.ConfigStore.CONFIGS);
      List<Field<?>> primaryKey = getPrimaryKey(namespace, type, name);
      Optional<StructuredRow> row = table.read(primaryKey);
      return row.map(this::fromRow).orElseThrow(() -> new ConfigNotFoundException(namespace, type, name));
    }, ConfigNotFoundException.class);
  }

  @Override
  public void update(String namespace, String type, Config config) throws ConfigNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.ConfigStore.CONFIGS);
      List<Field<?>> primaryKey = getPrimaryKey(namespace, type, config.getName());
      Optional<StructuredRow> row = table.read(primaryKey);
      if (!row.isPresent()) {
        throw new ConfigNotFoundException(namespace, type, config.getName());
      }
      table.upsert(toFields(namespace, type, config));
    }, ConfigNotFoundException.class);
  }

  private Config fromRow(StructuredRow row) {
    String name = row.getString(StoreDefinition.ConfigStore.NAME_FIELD);
    Map<String, String> properties =
      GSON.fromJson(row.getString(StoreDefinition.ConfigStore.PROPERTIES_FIELD), MAP_TYPE);
    return new Config(name, properties);
  }

  private List<Field<?>> toFields(String namespace, String type, Config config) {
    List<Field<?>> fields = getPrimaryKey(namespace, type, config.getName());
    fields.add(Fields.stringField(StoreDefinition.ConfigStore.PROPERTIES_FIELD, GSON.toJson(config.getProperties())));
    return fields;
  }

  private List<Field<?>> getPrimaryKey(String namespace, String type, String name) {
    List<Field<?>> primaryKey = new ArrayList<>();
    primaryKey.add(Fields.stringField(StoreDefinition.ConfigStore.NAMESPACE_FIELD, namespace));
    primaryKey.add(Fields.stringField(StoreDefinition.ConfigStore.TYPE_FIELD, type));
    primaryKey.add(Fields.stringField(StoreDefinition.ConfigStore.NAME_FIELD, name));
    return primaryKey;
  }
}
