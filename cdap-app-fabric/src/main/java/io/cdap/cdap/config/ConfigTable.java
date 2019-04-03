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

package io.cdap.cdap.config;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Table methods for {@link DefaultConfigStore} and {@link PreferencesTable}.
 */
public class ConfigTable {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final StructuredTable table;

  public ConfigTable(StructuredTableContext context) {
    this.table = context.getTable(StoreDefinition.ConfigStore.CONFIGS);
  }

  public void create(String namespace, String type, Config config) throws IOException, ConfigExistsException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, config.getName());
    Optional<StructuredRow> row = table.read(primaryKey);
    if (row.isPresent()) {
      throw new ConfigExistsException(namespace, type, config.getName());
    }
    table.upsert(toFields(namespace, type, config));
  }

  public void createOrUpdate(String namespace, String type, Config config) throws IOException {
    table.upsert(toFields(namespace, type, config));
  }

  public void delete(String namespace, String type, String name) throws IOException, ConfigNotFoundException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, name);
    Optional<StructuredRow> row = table.read(primaryKey);
    if (!row.isPresent()) {
      throw new ConfigNotFoundException(namespace, type, name);
    }
    table.delete(primaryKey);
  }

  public List<Config> list(String namespace, String type) throws IOException {
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
  }

  public Config get(String namespace, String type, String name) throws IOException, ConfigNotFoundException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, name);
    Optional<StructuredRow> row = table.read(primaryKey);
    return row.map(this::fromRow).orElseThrow(() -> new ConfigNotFoundException(namespace, type, name));
  }

  public void update(String namespace, String type, Config config) throws IOException, ConfigNotFoundException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, config.getName());
    Optional<StructuredRow> row = table.read(primaryKey);
    if (!row.isPresent()) {
      throw new ConfigNotFoundException(namespace, type, config.getName());
    }
    table.upsert(toFields(namespace, type, config));
  }

  private Config fromRow(StructuredRow row) {
    String name = row.getString(StoreDefinition.ConfigStore.NAME_FIELD);
    String string = row.getString(StoreDefinition.ConfigStore.PROPERTIES_FIELD);
    Map<String, String> properties = string != null ? GSON.fromJson(string, MAP_TYPE) : Collections.emptyMap();
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
