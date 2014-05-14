package com.continuuity.data2.dataset2.module.lib.inmemory;

import com.continuuity.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableDefinition;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

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
