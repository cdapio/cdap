/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.leveldb.LevelDBOVCTable;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.concurrent.ExecutionException;

/**
 * This class implements the table handle for LevelDB.
 */
public class LevelDBFilterableOVCTableHandle extends SimpleOVCTableHandle {
  @Inject
  @Named("LevelDBOVCTableHandleBasePath")
  private String basePath;

  @Inject
  @Named("LevelDBOVCTableHandleBlockSize")
  private Integer blockSize;

  @Inject
  @Named("LevelDBOVCTableHandleCacheSize")
  private Long cacheSize;

  private final LoadingCache<String, LevelDBOVCTable> tableCache;

  /**
   * This class is a singleton.
   * We have to guard against creating multiple instances because level db supports only one active client
   */
  private static final LevelDBFilterableOVCTableHandle INSTANCE = new LevelDBFilterableOVCTableHandle();

  private LevelDBFilterableOVCTableHandle() {
    tableCache = CacheBuilder.newBuilder().build(new CacheLoader<String, LevelDBOVCTable>() {
      @Override
      public LevelDBOVCTable load(String tableName) throws Exception {
        return openOrCreateTable(tableName);
      }
    });
  }

  public static LevelDBFilterableOVCTableHandle getInstance() {
    return INSTANCE;
  }

  @Override
  protected OrderedVersionedColumnarTable createNewTable(byte[] tableName) throws OperationException {
    try {
      return tableCache.get(Bytes.toString(tableName));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof OperationException) {
        throw (OperationException) cause;
      }
      throw new OperationException(StatusCode.INTERNAL_ERROR, cause.getMessage(), cause);
    }
  }

  @Override
  protected OrderedVersionedColumnarTable openTable(byte[] tableName) throws OperationException {
    return tableCache.getIfPresent(Bytes.toString(tableName));
  }

  /**
   * Opens the table if it already exists or creates a new one if it doesn't.
   * @param tableName The name of the table.
   * @return A LevelDBOVCTable.
   * @throws com.continuuity.api.data.OperationException If there is any error when try to open or create the table.
   */
  private LevelDBFilterableOVCTable openOrCreateTable(String tableName) throws OperationException {
    LevelDBFilterableOVCTable table = new LevelDBFilterableOVCTable(basePath, tableName, blockSize, cacheSize);

    if (table.openTable()) {
      return table;
    }
    table.initializeTable();
    return table;
  }

  @Override
  public String getName() {
    return "leveldb";
  }
}
