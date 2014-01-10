package com.continuuity.data2.dataset2.module.lib.inmemory;

import com.continuuity.api.data.module.DatasetModule;
import com.continuuity.api.data.module.DatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableDefinition;

/**
 * Registers in-memory implementations of the basic datasets
 */
public class InMemoryTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    InMemoryOrderedTableDefinition orderedTableDataset = new InMemoryOrderedTableDefinition("orderedTable");
    registry.add(orderedTableDataset);
  }
}
