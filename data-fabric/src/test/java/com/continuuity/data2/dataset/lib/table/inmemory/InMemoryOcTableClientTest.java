package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;

/**
 *
 */
public class InMemoryOcTableClientTest extends BufferingOcTableClientTest<InMemoryOcTableClient> {
  @Override
  protected InMemoryOcTableClient getTable(String name) {
    return new InMemoryOcTableClient(name);
  }

  @Override
  protected DataSetManager getTableManager() {
    return new InMemoryOcTableManager();
  }
}
