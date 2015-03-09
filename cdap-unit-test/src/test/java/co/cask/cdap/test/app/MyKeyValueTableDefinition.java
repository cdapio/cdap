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

package co.cask.cdap.test.app;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.batch.SplitReaderAdapter;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.CompositeDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Custom dataset example: key-value table
 */
public class MyKeyValueTableDefinition
  extends CompositeDatasetDefinition<MyKeyValueTableDefinition.KeyValueTable> {

  public MyKeyValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
    super(name, ImmutableMap.of("table", tableDefinition));
  }

  @Override
  public MyKeyValueTableDefinition.KeyValueTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                                            Map<String, String> arguments,
                                                            ClassLoader classLoader) throws IOException {
    return new MyKeyValueTableDefinition.KeyValueTable(spec.getName(),
                                                       getDataset(datasetContext, "table", Table.class,
                                                                  spec, arguments, classLoader));
  }

  /**
   * Custom dataset example: key-value table
   */
  public static class KeyValueTable
    extends AbstractDataset implements BatchReadable<String, String>, BatchWritable<String, String> {

    private static final String COL = "";

    private final Table table;

    public KeyValueTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    public void put(String key, String value) {
      table.put(new Put(key, COL, value));
    }

    public String get(String key) {
      return table.get(new Get(key, COL)).getString(COL);
    }

    public String get(String key, String defaultValue) {
      String value = get(key);
      return value == null ? defaultValue : value;
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public SplitReader<String, String> createSplitReader(Split split) {
      return new SplitReaderAdapter<byte[], String, Row, String> (table.createSplitReader(split)) {
        @Override
        protected String convertKey(byte[] key) {
          return Bytes.toString(key);
        }

        @Override
        protected String convertValue(Row value) {
          return value.getString(COL);
        }
      };
    }

    @Override
    public void write(String key, String value) {
      put(key, value);
    }
  }

  /**
   * Dataset module
   */
  public static class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> tableDefinition = registry.get("table");
      MyKeyValueTableDefinition keyValueTable = new MyKeyValueTableDefinition("myKeyValueTable", tableDefinition);
      registry.add(keyValueTable);
    }
  }
}

