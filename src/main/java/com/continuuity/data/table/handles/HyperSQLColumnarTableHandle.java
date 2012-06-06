/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.table.handles;

import java.sql.Connection;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.hypersql.HyperSQLOVCTable;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.converter.ColumnarOnVersionedColumnarTable;

public class HyperSQLColumnarTableHandle extends SimpleColumnarTableHandle {
  
  @Override
  public ColumnarTable createNewTable(byte[] tableName,
      TimestampOracle timeOracle) {
    return new ColumnarOnVersionedColumnarTable(
        new HyperSQLOVCTable(Bytes.toString(tableName), (Connection)null),
        timeOracle);
  }
}
