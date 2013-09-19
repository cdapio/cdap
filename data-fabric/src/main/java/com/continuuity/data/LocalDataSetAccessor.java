package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBMetricsTableClient;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableClient;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableManager;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class LocalDataSetAccessor extends AbstractDataSetAccessor {

  private final LevelDBOcTableService service;

  @Inject
  public LocalDataSetAccessor(@Named("DataFabricOperationExecutorConfig") CConfiguration conf,
                              LevelDBOcTableService service) {
    super(conf);
    this.service = service;
  }

  @Override
  protected <T> T getDataSetClient(String name, Class<? extends T> type) throws IOException {
    if (type == OrderedColumnarTable.class) {
      return (T) new LevelDBOcTableClient(name, service);
    }
    if (type == MetricsTable.class) {
      return (T) new LevelDBMetricsTableClient(name, service);
    }

    return null;
  }

  @Override
  protected DataSetManager getDataSetManager(Class type) throws IOException {
    if (type == OrderedColumnarTable.class) {
      return new LevelDBOcTableManager(service);
    }
    if (type == MetricsTable.class) {
      return new LevelDBOcTableManager(service);
    }

    return null;
  }

  @Override
  protected Map<String, Class<?>> list(String prefix) throws Exception {
    Map<String, Class<?>> datasets = Maps.newHashMap();
    for (String tableName : service.list()) {
      if (tableName.startsWith(prefix)) {
        datasets.put(tableName, OrderedColumnarTable.class);
      }
    }
    return datasets;
  }
}

