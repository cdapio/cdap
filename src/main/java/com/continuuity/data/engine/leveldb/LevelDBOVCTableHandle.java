/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.leveldb;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.SQLException;

/**
 * This class implements the table handle for LevelDB.
 */
public class LevelDBOVCTableHandle extends SimpleOVCTableHandle {

  private final String basePath;
  private final Integer blockSize;
  private final Long cacheSize;

  @Inject
  public LevelDBOVCTableHandle(
      @Named("LevelDBOVCTableHandleBasePath")String basePath,
      @Named("LevelDBOVCTableHandleBlockSize")Integer blockSize,
      @Named("LevelDBOVCTableHandleCacheSize")Long cacheSize)
          throws SQLException {
    this.basePath = basePath;
    this.blockSize = blockSize;
    this.cacheSize = cacheSize;
  }

  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    LevelDBOVCTable table =
        new LevelDBOVCTable(basePath, Bytes.toString(tableName), blockSize, cacheSize);
    table.initializeTable();
    return table;
  }

  @Override
  public OrderedVersionedColumnarTable openTable(byte[] tableName)
      throws OperationException {
    LevelDBOVCTable table =
        new LevelDBOVCTable(basePath, Bytes.toString(tableName), blockSize, cacheSize);
    if (table.openTable()) {
      return table;
    } else {
      return null;
    }
  }

  @Override
  public String getName() {
    return "leveldb";
  }
}
