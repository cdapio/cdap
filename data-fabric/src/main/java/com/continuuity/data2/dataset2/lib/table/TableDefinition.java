package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetProperties;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.data.dataset.lib.table.Table;

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
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec) throws IOException {
    return tableDef.getAdmin(spec.getSpecification(""));
  }

  @Override
  public Table getDataset(DatasetSpecification spec) throws IOException {
    OrderedTable table = tableDef.getDataset(spec.getSpecification(""));
    return new TableDataset(spec.getName(), table);
  }
}
