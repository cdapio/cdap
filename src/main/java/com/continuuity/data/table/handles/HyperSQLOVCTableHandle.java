/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.table.handles;

import java.sql.Connection;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.hypersql.HyperSQLOVCTable;
import com.continuuity.data.table.OrderedVersionedColumnarTable;

public class HyperSQLOVCTableHandle extends SimpleOVCTableHandle {
  
  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName) {
    return new HyperSQLOVCTable(Bytes.toString(tableName), (Connection)null);
  }
}
