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

/**
 * This class implements the table handle for LevelDB.
 */
public class LevelDBOVCTableHandle extends SimpleOVCTableHandle {
  @Inject
  @Named("LevelDBOVCTableHandleBasePath")
  private String basePath;

  @Inject
  @Named("LevelDBOVCTableHandleBlockSize")
  private Integer blockSize;

  @Inject
  @Named("LevelDBOVCTableHandleCacheSize")
  private Long cacheSize;

  /**
   * This class is a singleton.
   * We have to guard against creating multiple instances because level db supports only one active client
   */
  private static final LevelDBOVCTableHandle INSTANCE = new LevelDBOVCTableHandle();

  private LevelDBOVCTableHandle() {}

  public static LevelDBOVCTableHandle getInstance() {
    return INSTANCE;
  }

  @Override
  protected OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    LevelDBOVCTable table =
        new LevelDBOVCTable(basePath, Bytes.toString(tableName), blockSize, cacheSize);
    table.initializeTable();
    return table;
  }

  @Override
  protected OrderedVersionedColumnarTable openTable(byte[] tableName)
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
