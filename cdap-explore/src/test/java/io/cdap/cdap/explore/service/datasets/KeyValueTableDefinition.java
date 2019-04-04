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

package io.cdap.cdap.explore.service.datasets;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.RecordScannable;
import io.cdap.cdap.api.data.batch.RecordScanner;
import io.cdap.cdap.api.data.batch.RecordWritable;
import io.cdap.cdap.api.data.batch.Scannables;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.AbstractDataset;
import io.cdap.cdap.api.dataset.lib.AbstractDatasetDefinition;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class KeyValueTableDefinition
  extends AbstractDatasetDefinition<KeyValueTableDefinition.KeyValueTable, DatasetAdmin> {
  private static final Gson GSON = new Gson();

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public KeyValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
    super(name);
    this.tableDef = orderedTableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("key-value-table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(datasetContext, spec.getSpecification("key-value-table"), classLoader);
  }

  @Override
  public KeyValueTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                  Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table = tableDef.getDataset(datasetContext, spec.getSpecification("key-value-table"), arguments, classLoader);
    return new KeyValueTable(spec.getName(), table);
  }

  /**
   * KeyStructValueTable
   */
  public static class KeyValueTable extends AbstractDataset
    implements RecordScannable<KeyValue>, RecordWritable<KeyValue> {
    static final byte[] COL = new byte[] {'c', 'o', 'l', '1'};

    private final Table table;

    public KeyValueTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    public void put(int key, String value) throws Exception {
      table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
    }

    public String get(int key) throws Exception {
      return Bytes.toString(table.get(Bytes.toBytes(key), COL));
    }

    @Override
    public Type getRecordType() {
      return KeyValue.class;
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public RecordScanner<KeyValue> createSplitRecordScanner(Split split) {
      return Scannables.splitRecordScanner(table.createSplitReader(split), KEY_VALUE_ROW_MAKER);
    }

    @Override
    public void write(KeyValue keyValue) throws IOException {
      try {
        put(keyValue.getKey(), keyValue.getValue());
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public static class KeyValue {
    private final int key;
    private final String value;

    public KeyValue(Integer key, String value) {
      this.key = key;
      this.value = value;
    }

    @SuppressWarnings("UnusedDeclaration")
    public int getKey() {
      return key;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      KeyValue that = (KeyValue) o;

      return Objects.equal(this.key, that.key) &&
        Objects.equal(this.value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key, value);
    }
  }

  /**
   * KeyValueTableModule
   */
  public static class KeyValueTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
      KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("kvTable", table);
      registry.add(keyValueTable);
    }
  }

  private static final Scannables.RecordMaker<byte[], Row, KeyValue> KEY_VALUE_ROW_MAKER =
    new Scannables.RecordMaker<byte[], Row, KeyValue>() {
      @Override
      public KeyValue makeRecord(byte[] key, Row row) {
        return new KeyValue(Bytes.toInt(key),
                            GSON.fromJson(Bytes.toString(row.get(KeyValueTable.COL)), String.class));
      }
    };
}
