/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.leveldb;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.SQLException;

/**
 * A hybrid {@link com.continuuity.data.table.OVCTableHandle} that primarily uses LevelDB tables, except
 * for the queue table, which uses a Memory table. The stream table still uses LevelDB for now.
 */
public class LevelDBAndMemoryOVCTableHandle extends LevelDBOVCTableHandle {

  @Inject
  public LevelDBAndMemoryOVCTableHandle(
      @Named("LevelDBOVCTableHandleBasePath")String basePath,
      @Named("LevelDBOVCTableHandleBlockSize")Integer blockSize,
      @Named("LevelDBOVCTableHandleCacheSize")Long cacheSize)
          throws SQLException {
    super(basePath, blockSize, cacheSize);
  }

  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    // If this is the queue table, use a memory table, otherwise leveldb
    if (Bytes.equals(tableName, SimpleOVCTableHandle.queueOVCTable)) {
      return new MemoryOVCTable(tableName);
    }
    return super.createNewTable(tableName);
  }

  @Override
  public String getName() {
    return "leveldb+memory";
  }
}
