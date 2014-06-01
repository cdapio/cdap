package com.continuuity.data2.dataset2.module.lib.leveldb;

import com.continuuity.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableDefinition;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * Registers LevelDB-based implementations of the basic datasets
 */
public class LevelDBTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    LevelDBOrderedTableDefinition orderedTableDataset = new LevelDBOrderedTableDefinition("orderedTable");
    registry.add(orderedTableDataset);
  }
}
