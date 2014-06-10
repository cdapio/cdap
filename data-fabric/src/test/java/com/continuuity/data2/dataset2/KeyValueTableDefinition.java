package com.continuuity.data2.dataset2;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetProperties;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import java.io.IOException;

/**
 *
 */
public class KeyValueTableDefinition
  extends AbstractDatasetDefinition<SimpleKVTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public KeyValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
    super(name);
    this.tableDef = orderedTableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("table"));
  }

  @Override
  public SimpleKVTable getDataset(DatasetSpecification spec) throws IOException {
    Table table = tableDef.getDataset(spec.getSpecification("table"));
    return new SimpleKVTable(spec, table);
  }

  /**
   * Module
   */
  public static class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> orderedTable = registry.get("table");
      KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("keyValueTable", orderedTable);
      registry.add(keyValueTable);
    }
  }
}
