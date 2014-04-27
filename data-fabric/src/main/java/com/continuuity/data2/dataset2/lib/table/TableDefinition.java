package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import java.io.IOException;

/**
 * Defines Table data set
 */
public class TableDefinition extends AbstractDatasetDefinition<Table, DatasetAdmin> {

  private final DatasetDefinition<? extends OrderedTable, ?> tableDef;

  public TableDefinition(String name, DatasetDefinition<? extends OrderedTable, ?> orderedTableDefinition) {
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
  public DatasetAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("table"));
  }

  @Override
  public Table getDataset(DatasetInstanceSpec spec) throws IOException {
    OrderedTable table = tableDef.getDataset(spec.getSpecification("table"));
    return new TableDataset(spec.getName(), table);
  }
}
