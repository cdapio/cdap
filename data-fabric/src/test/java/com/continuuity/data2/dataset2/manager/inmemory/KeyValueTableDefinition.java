package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.api.data.dataset2.DatasetAdmin;
import com.continuuity.api.data.dataset2.DatasetDefinition;
import com.continuuity.api.data.dataset2.DatasetInstanceProperties;
import com.continuuity.api.data.dataset2.DatasetInstanceSpec;
import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.api.data.dataset2.lib.table.OrderedTable;
import com.continuuity.api.data.module.DatasetModule;
import com.continuuity.api.data.module.DatasetDefinitionRegistry;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class KeyValueTableDefinition
  extends AbstractDatasetDefinition<KeyValueTableDefinition.KeyValueTable, DatasetAdmin> {

  private final DatasetDefinition<? extends OrderedTable, ?> tableDef;

  public KeyValueTableDefinition(String name, DatasetDefinition<? extends OrderedTable, ?> orderedTableDefinition) {
    super(name);
    this.tableDef = orderedTableDefinition;
  }

  @Override
  public DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties.getProperties("table")))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetInstanceSpec spec) throws Exception {
    return tableDef.getAdmin(spec.getSpecification("table"));
  }

  @Override
  public KeyValueTable getDataset(DatasetInstanceSpec spec) throws Exception {
    OrderedTable table = tableDef.getDataset(spec.getSpecification("table"));
    return new KeyValueTable(table);
  }

  /**
   * KeyValueTable
   */
  public static class KeyValueTable extends AbstractDataset {
    private static final byte[] COL = new byte[0];

    private final OrderedTable table;

    public KeyValueTable(OrderedTable table) {
      super(table);
      this.table = table;
    }

    public void put(String key, String value) throws Exception {
      table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
    }

    public String get(String key) throws Exception {
      return Bytes.toString(table.get(Bytes.toBytes(key), COL));
    }
  }

  /**
   * KeyValueTableModule
   */
  public static class KeyValueTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<OrderedTable, DatasetAdmin> orderedTable = registry.get("orderedTable");
      KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("keyValueTable", orderedTable);
      registry.add(keyValueTable);
    }
  }
}
