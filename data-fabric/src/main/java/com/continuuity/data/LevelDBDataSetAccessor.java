package com.continuuity.data;

import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableManager;
import com.continuuity.data2.dataset.api.DataSetClient;

/**
 *
 */
public class LevelDBDataSetAccessor implements DataSetAccessor {

  @Override
  public DataSetClient getDataSetClient(String name, Class type) {
    if (type == OrderedColumnarTable.class) {
      return new InMemoryOcTableClient(name);
    }

    return null;
  }

  @Override
  public DataSetManager getDataSetManager(Class type) {
    if (type == OrderedColumnarTable.class) {
      return new InMemoryOcTableManager();
    }

    return null;
  }
}

