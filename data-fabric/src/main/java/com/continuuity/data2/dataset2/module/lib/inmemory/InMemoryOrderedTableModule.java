package com.continuuity.data2.dataset2.module.lib.inmemory;

import com.continuuity.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableDefinition;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * Registers in-memory implementations of the basic datasets
 */
public class InMemoryOrderedTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new InMemoryOrderedTableDefinition("orderedTable"));
    // so that it can be resolved via @Dataset
    registry.add(new InMemoryOrderedTableDefinition(OrderedTable.class.getName()));
  }
}
