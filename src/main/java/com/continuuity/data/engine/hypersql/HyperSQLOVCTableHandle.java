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

/**
 * This class implements the table handle for HyperSQL.
 */
public class HyperSQLOVCTableHandle extends SimpleOVCTableHandle {

  private final Connection connection;

  @Inject
  public HyperSQLOVCTableHandle(
      @Named("HyperSQLOVCTableHandleJDBCString")String hyperSqlJDBCString)
          throws SQLException {
    this.connection = DriverManager.getConnection(hyperSqlJDBCString);
  }

  @Override
  protected OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    HyperSQLOVCTable table = new HyperSQLOVCTable(Bytes.toString(tableName), this.connection);
    table.initializeTable();
    return table;
  }

  @Override
  protected OrderedVersionedColumnarTable openTable(byte[] tableName) throws OperationException {
    HyperSQLOVCTable table =
        new HyperSQLOVCTable(Bytes.toString(tableName), this.connection);
    if (table.openTable()) {
      return table;
    } else {
      return null;
    }
  }

  @Override
  public String getName() {
    return "hSQL";
  }
}
