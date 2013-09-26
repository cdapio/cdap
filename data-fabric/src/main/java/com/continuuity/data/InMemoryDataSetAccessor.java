package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryMetricsTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableManager;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableService;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Map;

/**
 * Provides access to datasets in in-memory mode.
 */
public class InMemoryDataSetAccessor extends AbstractDataSetAccessor {
  @Inject
  public InMemoryDataSetAccessor(@Named("DataSetAccessorConfig") CConfiguration conf) {
    super(conf);
  }

  @Override
  protected <T> T getOcTableClient(String name, ConflictDetection level) throws Exception {
    return (T) new InMemoryOcTableClient(name, level);
  }

  @Override
  protected DataSetManager getOcTableManager() throws Exception {
    return new InMemoryOcTableManager();
  }

  @Override
  protected <T> T getMetricsTableClient(String name) throws Exception {
    return (T) new InMemoryMetricsTableClient(name);
  }

  @Override
  protected DataSetManager getMetricsTableManager() throws Exception {
    return new InMemoryOcTableManager();
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

