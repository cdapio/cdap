package com.continuuity.data2.dataset2.module.lib.hbase;

import com.continuuity.data2.dataset2.lib.table.hbase.HBaseOrderedTable;
import com.continuuity.data2.dataset2.lib.table.hbase.HBaseOrderedTableDefinition;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * Registers HBase-backed implementations of the basic datasets
 */
public class HBaseOrderedTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new HBaseOrderedTableDefinition("orderedTable"));
    // so that it can be resolved via @Dataset
    registry.add(new HBaseOrderedTableDefinition(HBaseOrderedTable.class.getName()));
  }
}
