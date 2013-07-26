/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;

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
  protected synchronized OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    LevelDBOVCTable table =
        new LevelDBOVCTable(basePath, Bytes.toString(tableName), blockSize, cacheSize);

    // Always try to open table first, as it's possible that when two threads calling this method at the same time,
    // the one get executed first (this method is synchronized) will initialized the table, leaving the
    // latter one get an exception if it calls initializeTable directly.
    if (table.openTable()) {
      return table;
    }
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
