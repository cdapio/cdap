/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;

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
  public DatasetAdmin getAdmin(DatasetContext datasetContext, ClassLoader classLoader,
                               DatasetSpecification spec) throws IOException {
    return tableDef.getAdmin(datasetContext, classLoader, spec.getSpecification("table"));
  }

  @Override
  public PrefixedTable getDataset(DatasetContext datasetContext, Map<String, String> arguments, ClassLoader classLoader,
                                  DatasetSpecification spec) throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("table");
    KeyValueTable table = tableDef.getDataset(datasetContext, DatasetDefinition.NO_ARGUMENTS, classLoader, kvTableSpec);
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
