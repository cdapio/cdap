package com.continuuity.data2.dataset2.module.lib.leveldb;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.data2.dataset2.lib.table.leveldb.LevelDBOrderedTable;
import com.continuuity.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableDefinition;

/**
 * Registers LevelDB-based implementations of the basic datasets
 */
public class LevelDBOrderedTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new LevelDBOrderedTableDefinition("orderedTable"));
    // so that it can be resolved via @Dataset
    registry.add(new LevelDBOrderedTableDefinition(OrderedTable.class.getName()));
  }
}
