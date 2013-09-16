package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableManager;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableService;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Map;

/**
 *
 */
public class InMemoryDataSetAccessor extends AbstractDataSetAccessor {
  @Inject
  public InMemoryDataSetAccessor(@Named("DataFabricOperationExecutorConfig") CConfiguration conf) {
    super(conf);
  }

  @Override
  public <T> T getDataSetClient(String name, Class<? extends T> type) {
    if (type == OrderedColumnarTable.class) {
      return (T) new InMemoryOcTableClient(name);
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

  @Override
  protected Map<String, Class<?>> list(String prefix) throws Exception {
    Map<String, Class<?>> datasets = Maps.newHashMap();
    for (String tableName : InMemoryOcTableService.list()) {
      if (tableName.startsWith(prefix)) {
        datasets.put(tableName, OrderedColumnarTable.class);
      }
    }
    return datasets;
  }
}

