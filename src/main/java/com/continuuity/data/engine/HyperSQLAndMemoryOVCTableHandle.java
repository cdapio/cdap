package com.continuuity.data.engine;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.hypersql.HyperSQLOVCTableHandle;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * A hybrid {@link OVCTableHandle} that primarily uses HyperSQL tables, except
 * for the queue table, which uses a Memory table.  The stream table still uses
 * HyperSQL for now.
 */
public class HyperSQLAndMemoryOVCTableHandle extends HyperSQLOVCTableHandle {

  @Inject
  public HyperSQLAndMemoryOVCTableHandle(
      @Named("HyperSQLOVCTableHandleJDBCString")String hyperSqlJDBCString,
      @Named("HyperSQLOVCTableHandleProperties")Properties hyperSqlProperties)
          throws SQLException {
    super(hyperSqlJDBCString, hyperSqlProperties);
  }

  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName) {
    // If this is the queue table, use a memory table, otherwise hypersql
    if (Bytes.equals(tableName, SimpleOVCTableHandle.queueOVCTable)) {
      return new MemoryOVCTable(tableName);
    }
    return super.createNewTable(tableName);
  }

}
