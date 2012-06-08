/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hypersql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class HyperSQLOVCTableHandle extends SimpleOVCTableHandle {
  
  private final String hyperSqlJDBCString;
  private final Properties hyperSqlProperties;
  private final Connection connection;
  
  @Inject
  public HyperSQLOVCTableHandle(
      @Named("HyperSQLOVCTableHandleJDBCString")String hyperSqlJDBCString,
      @Named("HyperSQLOVCTableHandleProperties")Properties hyperSqlProperties)
          throws SQLException {
    this.hyperSqlJDBCString = hyperSqlJDBCString;
    this.hyperSqlProperties = hyperSqlProperties;
    this.connection = DriverManager.getConnection(this.hyperSqlJDBCString,
        this.hyperSqlProperties);
  }
  
  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName) {
    HyperSQLOVCTable table =
        new HyperSQLOVCTable(Bytes.toString(tableName), this.connection);
    table.initializeTable();
    return table;
  }
}
