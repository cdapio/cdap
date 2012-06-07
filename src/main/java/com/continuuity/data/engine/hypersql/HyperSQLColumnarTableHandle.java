/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hypersql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.SimpleColumnarTableHandle;
import com.continuuity.data.table.converter.ColumnarOnVersionedColumnarTable;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class HyperSQLColumnarTableHandle extends SimpleColumnarTableHandle {
  
  private final String hyperSqlJDBCString;
  private final Connection connection;
  
  @Inject
  public HyperSQLColumnarTableHandle(
      @Named("HyperSQLOVCTableHandleJDBCString")String hyperSqlJDBCString)
          throws SQLException {
    this.hyperSqlJDBCString = hyperSqlJDBCString;
    this.connection = DriverManager.getConnection(this.hyperSqlJDBCString,
        "sa", "");
  }
  
  @Override
  public ColumnarTable createNewTable(byte[] tableName,
      TimestampOracle timeOracle) {
    return new ColumnarOnVersionedColumnarTable(
        new HyperSQLOVCTable(Bytes.toString(tableName), this.connection),
        timeOracle);
  }
}
