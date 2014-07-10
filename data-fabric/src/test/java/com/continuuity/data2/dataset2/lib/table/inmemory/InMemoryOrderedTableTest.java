package com.continuuity.data2.dataset2.lib.table.inmemory;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;
import com.continuuity.data2.dataset2.lib.table.BufferingOrederedTableTest;

/**
 *
 */
public class InMemoryOrderedTableTest extends BufferingOrederedTableTest<InMemoryOcTableClient> {
  @Override
  protected InMemoryOcTableClient getTable(String name, ConflictDetection conflictLevel) throws Exception {
    return new InMemoryOcTableClient(name,
                       com.continuuity.data2.dataset.lib.table.ConflictDetection.valueOf(conflictLevel.name()));
  }

  @Override
  protected DatasetAdmin getTableAdmin(String name) throws Exception {
    return new InMemoryOrderedTableAdmin(name);
  }
}
