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
public class KeyStructValueTableDefinition
  extends AbstractDatasetDefinition<KeyStructValueTableDefinition.KeyStructValueTable, DatasetAdmin> {
  private static final Gson GSON = new Gson();

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public KeyStructValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
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
  public DatasetAdmin getAdmin(DatasetContext datasetContext, ClassLoader classLoader,
                               DatasetSpecification spec) throws IOException {
    return tableDef.getAdmin(datasetContext, classLoader, spec.getSpecification("key-value-table"));
  }

  @Override
  public KeyStructValueTable getDataset(DatasetContext datasetContext, Map<String, String> arguments,
                                        ClassLoader classLoader, DatasetSpecification spec) throws IOException {
    Table table = tableDef.getDataset(datasetContext, arguments, classLoader, spec.getSpecification("key-value-table"));
    return new KeyStructValueTable(spec.getName(), table);
  }

  /**
   * KeyStructValueTable
   */
  public static class KeyStructValueTable extends AbstractDataset
    implements RecordScannable<KeyValue>, RecordWritable<KeyValue> {
    static final byte[] COL = new byte[] {'c', 'o', 'l', '1'};

    private final Table table;

    public KeyStructValueTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    public void put(String key, KeyValue.Value value) throws Exception {
      table.put(Bytes.toBytes(key), COL, Bytes.toBytes(GSON.toJson(value)));
    }

    public KeyValue.Value get(String key) throws Exception {
      return GSON.fromJson(Bytes.toString(table.get(Bytes.toBytes(key), COL)), KeyValue.Value.class);
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
        put(keyValue.getKey() + "_2", keyValue.getValue());
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public static class KeyValue {
    private final String key;
    private final Value value;

    public KeyValue(String key, Value value) {
      this.key = key;
      this.value = value;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getKey() {
      return key;
    }

    @SuppressWarnings("UnusedDeclaration")
    public Value getValue() {
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

    public static class Value {
      private final String name;
      private final List<Integer> ints;

      public Value(String name, List<Integer> ints) {
        this.name = name;
        this.ints = ints;
      }

      @SuppressWarnings("UnusedDeclaration")
      public String getName() {
        return name;
      }

      @SuppressWarnings("UnusedDeclaration")
      public List<Integer> getInts() {
        return ints;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        Value that = (Value) o;

        return Objects.equal(this.name, that.name) &&
          Objects.equal(this.ints, that.ints);
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(name, ints);
      }
    }
  }

  /**
   * KeyStructValueTableModule
   */
  public static class KeyStructValueTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
      KeyStructValueTableDefinition keyValueTable = new KeyStructValueTableDefinition("keyStructValueTable", table);
      registry.add(keyValueTable);
    }
  }

  private static final Scannables.RecordMaker<byte[], Row, KeyValue> KEY_VALUE_ROW_MAKER =
    new Scannables.RecordMaker<byte[], Row, KeyValue>() {
      @Override
      public KeyValue makeRecord(byte[] key, Row row) {
        return new KeyValue(Bytes.toString(key),
                            GSON.fromJson(Bytes.toString(row.get(KeyStructValueTable.COL)), KeyValue.Value.class));
      }
    };
}
