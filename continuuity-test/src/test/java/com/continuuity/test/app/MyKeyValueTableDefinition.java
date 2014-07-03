package com.continuuity.test.app;

import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.batch.SplitReaderAdapter;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.lib.CompositeDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Custom dataset example: key-value table
 */
public class MyKeyValueTableDefinition
  extends CompositeDatasetDefinition<MyKeyValueTableDefinition.KeyValueTable> {

  public MyKeyValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
    super(name, ImmutableMap.of("table", tableDefinition));
  }

  @Override
  public MyKeyValueTableDefinition.KeyValueTable getDataset(DatasetSpecification spec,
                                                            ClassLoader classLoader) throws IOException {
    return new MyKeyValueTableDefinition.KeyValueTable(spec.getName(),
                                                       getDataset("table", Table.class, spec, classLoader));
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

