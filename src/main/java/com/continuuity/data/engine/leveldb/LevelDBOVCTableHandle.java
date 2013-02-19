/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.leveldb;

import java.sql.SQLException;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class LevelDBOVCTableHandle extends SimpleOVCTableHandle {

  private final String basePath;

  @Inject
  public LevelDBOVCTableHandle(
      @Named("LevelDBOVCTableHandleBasePath")String basePath)
          throws SQLException {
    this.basePath = basePath;
  }

  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    LevelDBOVCTable table =
        new LevelDBOVCTable(basePath, Bytes.toString(tableName));
    table.initializeTable();
    return table;
  }

  @Override
  public OrderedVersionedColumnarTable openTable(byte[] tableName)
      throws OperationException {
    LevelDBOVCTable table =
        new LevelDBOVCTable(basePath, Bytes.toString(tableName));
    if (table.openTable()) return table; else return null;
  }

  @Override
  public String getName() {
    return "leveldb";
  }
}
