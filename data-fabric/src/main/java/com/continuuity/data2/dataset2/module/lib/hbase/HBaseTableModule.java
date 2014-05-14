package com.continuuity.data2.dataset2.module.lib.hbase;

import com.continuuity.data2.dataset2.lib.table.hbase.HBaseOrderedTableDefinition;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * Registers HBase-backed implementations of the basic datasets
 */
public class HBaseTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    // todo: make dataset module HBaseConfigurationAware instead of individual DatasetDefinition?
    HBaseOrderedTableDefinition orderedTableDataset = new HBaseOrderedTableDefinition("orderedTable");
    registry.add(orderedTableDataset);
  }
}
