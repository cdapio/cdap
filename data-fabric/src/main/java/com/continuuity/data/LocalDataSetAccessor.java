package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableClient;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableManager;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;

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
  public DataSetClient getDataSetClient(String name, Class type) throws IOException {
    if (type == OrderedColumnarTable.class) {
      return new LevelDBOcTableClient(name, service);
    }

    return null;
  }

  @Override
  public DataSetManager getDataSetManager(Class type) throws IOException {
    if (type == OrderedColumnarTable.class) {
      return new LevelDBOcTableManager(service);
    }

    return null;
  }
}

