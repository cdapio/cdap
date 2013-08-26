/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.table.AbstractOVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;

/**
 * A hybrid {@link com.continuuity.data.table.OVCTableHandle} that primarily uses LevelDB tables, except
 * for the queue table, which uses a Memory table. The stream table still uses LevelDB for now.
 */
public class LevelDBAndMemoryOVCTableHandle extends AbstractOVCTableHandle {
  @Inject
  private LevelDBOVCTableHandle levelDBOVCTableHandle;
  @Inject
  private MemoryOVCTableHandle memoryOVCTableHandle;

  @Override
  public OrderedVersionedColumnarTable getTable(byte[] tableName)
      throws OperationException {
    // If this is the queue table, use a memory table, otherwise leveldb
    if (Bytes.equals(tableName, SimpleOVCTableHandle.QUEUE_OVC_TABLES)) {
      return memoryOVCTableHandle.getTable(tableName);
    }
    return levelDBOVCTableHandle.getTable(tableName);
  }

  @Override
  public String getName() {
    return "leveldb+memory";
  }
}
