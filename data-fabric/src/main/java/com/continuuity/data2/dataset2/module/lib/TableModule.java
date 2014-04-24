package com.continuuity.data2.dataset2.module.lib;

import com.continuuity.data2.dataset2.lib.table.TableDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 *
 */
public class TableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<OrderedTable, DatasetAdmin> orderedTable = registry.get("orderedTable");
    TableDefinition keyValueTable = new TableDefinition("table", orderedTable);
    registry.add(keyValueTable);
  }
}
