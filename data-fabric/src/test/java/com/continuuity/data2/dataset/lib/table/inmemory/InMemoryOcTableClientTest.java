package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;

/**
 *
 */
public class InMemoryOcTableClientTest extends BufferingOcTableClientTest<InMemoryOcTableClient> {
  @Override
  protected InMemoryOcTableClient getTable(String name, ConflictDetection level) {
    return new InMemoryOcTableClient(name, level);
  }

  @Override
  protected DataSetManager getTableManager() {
    return new InMemoryOcTableManager();
  }
}
