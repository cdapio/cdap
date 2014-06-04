/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.leveldb;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * Factory for creating {@link StreamConsumerStateStore} in level db.
 */
public final class LevelDBStreamConsumerStateStoreFactory implements StreamConsumerStateStoreFactory {

  private final LevelDBOcTableService tableService;
  private final String tableName;
  private LevelDBOcTableCore coreTable;

  @Inject
  LevelDBStreamConsumerStateStoreFactory(DataSetAccessor dataSetAccessor, LevelDBOcTableService tableService) {
    this.tableService = tableService;
    this.tableName = dataSetAccessor.namespace(QueueConstants.STREAM_TABLE_PREFIX,
                                               DataSetAccessor.Namespace.SYSTEM) + ".state.store";

  }

  @Override
  public synchronized StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException {
    if (coreTable == null) {
      tableService.ensureTableExists(tableName);
      coreTable = new LevelDBOcTableCore(tableName, tableService);
    }
    return new LevelDBStreamConsumerStateStore(streamConfig, coreTable);
  }

  @Override
  public synchronized void dropAll() throws IOException {
    coreTable = null;
    tableService.dropTable(tableName);
  }
}
