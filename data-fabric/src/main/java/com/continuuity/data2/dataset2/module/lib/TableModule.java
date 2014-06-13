package com.continuuity.data2.dataset2.module.lib;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.data2.dataset2.lib.table.TableDefinition;

/**
 *
 */
public class TableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<OrderedTable, DatasetAdmin> orderedTable = registry.get("orderedTable");
    registry.add(new TableDefinition("table", orderedTable));
    // so that it can be resolved via @Dataset
    registry.add(new TableDefinition(Table.class.getName(), orderedTable));
  }
}
