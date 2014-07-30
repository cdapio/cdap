/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.api.dataset.lib;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;

import java.io.IOException;
import java.util.Map;

/**
 * A dataset that accepts runtime arguments at instantiation time.
 * The arguments specify a key prefix that is prepended to all keys passed to read/write methods.
 */
public class PrefixedTableModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = registry.get("keyValueTable");
    DatasetDefinition definition = new PrefixedTableDefinition("prefixedTable", kvTableDef);
    registry.add(definition);
  }
}

class PrefixedTableDefinition extends AbstractDatasetDefinition<PrefixedTable, DatasetAdmin> {

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public PrefixedTableDefinition(String name, DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef) {
    super(name);
    this.tableDef = kvTableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("table"), classLoader);
  }

  @Override
  public PrefixedTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return getDataset(spec, null, classLoader);
  }

  @Override
  public PrefixedTable getDataset(DatasetSpecification spec, Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("table");
    KeyValueTable table = tableDef.getDataset(kvTableSpec, classLoader);
    return new PrefixedTable(spec.getName(), table, arguments);
  }
}

class PrefixedTable extends AbstractDataset {

  private final KeyValueTable table;
  private final String prefix;

  public PrefixedTable(String instanceName, KeyValueTable kvTable, Map<String, String> arguments) {
    super(instanceName, kvTable);
    table = kvTable;
    prefix = arguments == null ? null : arguments.get("prefix");
  }

  private String makeKey(String originalKey) {
    return prefix == null ? originalKey : prefix + originalKey;
  }

  public void write(String key, String value) {
    table.write(makeKey(key), value);
  }

  public String read(String key) {
    return Bytes.toString(table.read(makeKey(key)));
  }
}
