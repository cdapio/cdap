package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Table;

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
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification(""), classLoader);
  }

  @Override
  public Table getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    OrderedTable table = tableDef.getDataset(spec.getSpecification(""), classLoader);
    return new TableDataset(spec.getName(), table);
  }
}
