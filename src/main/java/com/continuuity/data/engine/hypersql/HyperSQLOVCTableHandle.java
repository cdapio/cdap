/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hypersql;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HyperSQLOVCTableHandle extends SimpleOVCTableHandle {

  private final String hyperSqlJDBCString;
  private final Connection connection;

  @Inject
  public HyperSQLOVCTableHandle(
      @Named("HyperSQLOVCTableHandleJDBCString")String hyperSqlJDBCString)
          throws SQLException {
    this.hyperSqlJDBCString = hyperSqlJDBCString;
    this.connection = DriverManager.getConnection(this.hyperSqlJDBCString);
  }

  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    HyperSQLOVCTable table = new HyperSQLOVCTable(Bytes.toString(tableName), this.connection);
    table.initializeTable();
    return table;
  }

  @Override
  public OrderedVersionedColumnarTable openTable(byte[] tableName) throws OperationException {
    HyperSQLOVCTable table =
        new HyperSQLOVCTable(Bytes.toString(tableName), this.connection);
    if (table.openTable()) return table; else return null;
  }

  @Override
  public String getName() {
    return "hSQL";
  }
}
