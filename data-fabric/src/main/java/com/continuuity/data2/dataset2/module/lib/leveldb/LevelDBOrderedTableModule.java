package com.continuuity.data2.dataset2.module.lib.leveldb;

import com.continuuity.data2.dataset2.lib.table.leveldb.LevelDBOrderedTable;
import com.continuuity.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableDefinition;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * Registers LevelDB-based implementations of the basic datasets
 */
public class LevelDBOrderedTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new LevelDBOrderedTableDefinition("orderedTable"));
    // so that it can be resolved via @Dataset
    registry.add(new LevelDBOrderedTableDefinition(LevelDBOrderedTable.class.getName()));
  }
}
