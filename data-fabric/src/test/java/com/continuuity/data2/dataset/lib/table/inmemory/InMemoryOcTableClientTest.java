package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTableTest;

/**
 *
 */
public class InMemoryOcTableClientTest extends OrderedColumnarTableTest {
  @Override
  protected OrderedColumnarTable getTable(String name) {
    return new InMemoryOcTableClient(name);
  }

  @Override
  protected DataSetManager getTableManager() {
    return new InMemoryOcTableManager();
  }
}
