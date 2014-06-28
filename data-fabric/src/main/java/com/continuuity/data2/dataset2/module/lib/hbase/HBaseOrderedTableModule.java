package com.continuuity.data2.dataset2.module.lib.hbase;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.data2.dataset2.lib.table.hbase.HBaseOrderedTable;
import com.continuuity.data2.dataset2.lib.table.hbase.HBaseOrderedTableDefinition;

/**
 * Registers HBase-backed implementations of the basic datasets
 */
public class HBaseOrderedTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new HBaseOrderedTableDefinition("orderedTable"));
    // so that it can be resolved via @Dataset
    registry.add(new HBaseOrderedTableDefinition(OrderedTable.class.getName()));
  }
}
