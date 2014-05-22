package com.continuuity.hive.data2;

import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitRowScanner;
import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.Row;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Simple key value table for testing.
 */
public class KeyValueTableDefinition
  extends AbstractDatasetDefinition<KeyValueTableDefinition.KeyValueTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public KeyValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
    super(name);
    this.tableDef = orderedTableDefinition;
  }

  @Override
  public DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("key-value-table", properties.getProperties("key-value-table")))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("key-value-table"));
  }

  @Override
  public KeyValueTable getDataset(DatasetInstanceSpec spec) throws IOException {
    Table table = tableDef.getDataset(spec.getSpecification("key-value-table"));
    return new KeyValueTable(spec.getName(), table);
  }

  /**
   * KeyValueTable
   */
  public static class KeyValueTable extends AbstractDataset implements RowScannable<KeyValue> {
    static final byte[] COL = new byte[] {'c', 'o', 'l', '1'};

    private final Table table;

    public KeyValueTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    public void put(String key, String value) throws Exception {
      table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
    }

    public String get(String key) throws Exception {
      return Bytes.toString(table.get(Bytes.toBytes(key), COL));
    }

    @Override
    public Type getRowType() {
      return KeyValue.class;
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public SplitRowScanner<KeyValue> createSplitScanner(Split split) {
      return Scannables.splitRowScanner(table.createSplitReader(split), KEY_VALUE_ROW_MAKER);
    }
  }

  public static class KeyValue {
    private final String key;
    private final String value;

    public KeyValue(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getKey() {
      return key;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getValue() {
      return value;
    }
  }

  /**
   * KeyValueTableModule
   */
  public static class KeyValueTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
      KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("keyValueTable", table);
      registry.add(keyValueTable);
    }
  }

  private static final Scannables.RowMaker<byte[], Row, KeyValue> KEY_VALUE_ROW_MAKER =
    new Scannables.RowMaker<byte[], Row, KeyValue>() {
      @Override
      public KeyValue makeRow(byte[] key, Row row) {
        return new KeyValue(Bytes.toString(key), Bytes.toString(row.get(KeyValueTable.COL)));
      }
    };
}
