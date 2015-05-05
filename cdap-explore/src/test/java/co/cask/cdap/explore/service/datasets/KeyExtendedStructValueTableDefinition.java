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

package co.cask.cdap.explore.service.datasets;

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Simple key value table for testing.
 */
public class KeyExtendedStructValueTableDefinition extends
  AbstractDatasetDefinition<KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTable, DatasetAdmin> {

  private static final Gson GSON = new Gson();
  private final DatasetDefinition<? extends Table, ?> tableDef;

  public KeyExtendedStructValueTableDefinition(String name,
                                               DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
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
  public KeyExtendedStructValueTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                                Map<String, String> arguments,
                                                ClassLoader classLoader) throws IOException {
    Table table = tableDef.getDataset(datasetContext, spec.getSpecification("key-value-table"), arguments, classLoader);
    return new KeyExtendedStructValueTable(spec.getName(), table);
  }

  /**
   * KeyStructValueTable
   */
  public static class KeyExtendedStructValueTable extends AbstractDataset
    implements RecordScannable<KeyExtendedValue>, RecordWritable<KeyExtendedValue> {

    static final byte[] COL = new byte[] {'c', 'o', 'l', '1'};
    private final Table table;

    public KeyExtendedStructValueTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    public void put(String key, KeyExtendedValue value) throws Exception {
      table.put(Bytes.toBytes(key), COL, Bytes.toBytes(GSON.toJson(value)));
    }

    public KeyExtendedValue get(String key) throws Exception {
      return GSON.fromJson(Bytes.toString(table.get(Bytes.toBytes(key), COL)), KeyExtendedValue.class);
    }

    @Override
    public Type getRecordType() {
      return KeyExtendedValue.class;
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public RecordScanner<KeyExtendedValue> createSplitRecordScanner(Split split) {
      return Scannables.splitRecordScanner(table.createSplitReader(split), KEY_VALUE_ROW_MAKER);
    }

    @Override
    public void write(KeyExtendedValue keyValue) throws IOException {
      try {
        put(keyValue.getKey() + "_2", keyValue);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public static class KeyExtendedValue {
    private final String key;
    private final KeyStructValueTableDefinition.KeyValue.Value value;
    private final int count;

    public KeyExtendedValue(String key, KeyStructValueTableDefinition.KeyValue.Value value, int count) {
      this.key = key;
      this.value = value;
      this.count = count;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getKey() {
      return key;
    }

    @SuppressWarnings("UnusedDeclaration")
    public KeyStructValueTableDefinition.KeyValue.Value getValue() {
      return value;
    }

    @SuppressWarnings("UnusedDeclaration")
    public int getCount() {
      return count;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      KeyExtendedValue that = (KeyExtendedValue) o;

      return Objects.equal(this.key, that.key) &&
        Objects.equal(this.value, that.value) &&
        Objects.equal(this.count, that.count);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key, value, count);
    }
  }

  /**
   * KeyStructValueTableModule
   */
  public static class KeyExtendedStructValueTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
      KeyExtendedStructValueTableDefinition keyExtendedValueTable =
        new KeyExtendedStructValueTableDefinition("keyExtendedStructValueTable", table);
      registry.add(keyExtendedValueTable);
    }
  }

  private static final Scannables.RecordMaker<byte[], Row, KeyExtendedValue> KEY_VALUE_ROW_MAKER =
    new Scannables.RecordMaker<byte[], Row, KeyExtendedValue>() {
      @Override
      public KeyExtendedValue makeRecord(byte[] key, Row row) {
        KeyExtendedValue value = GSON.fromJson(Bytes.toString(row.get(KeyExtendedStructValueTable.COL)),
                                               KeyExtendedValue.class);
        return new KeyExtendedValue(Bytes.toString(key),
                                    value.getValue(),
                                    value.getCount());
      }
    };
}
