package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBMetricsTableClient;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableClient;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableManager;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.Map;

/**
 *
 */
public class LocalDataSetAccessor extends AbstractDataSetAccessor {

  private final LevelDBOcTableService service;

  @Inject
  public LocalDataSetAccessor(CConfiguration conf,
                              LevelDBOcTableService service) {
    super(conf);
    this.service = service;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T getOcTableClient(String name, ConflictDetection level, int ttl) throws Exception {
    // ttl is ignored in local mode
    return (T) new LevelDBOcTableClient(name, level, service);
  }

  @Override
  protected DataSetManager getOcTableManager() throws Exception {
    return new LevelDBOcTableManager(service);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T getMetricsTableClient(String name) throws Exception {
    return (T) new LevelDBMetricsTableClient(name, service);
  }

  @Override
  protected DataSetManager getMetricsTableManager() throws Exception {
    return new LevelDBOcTableManager(service);
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

