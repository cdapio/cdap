package com.continuuity.data.engine.hypersql;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.SQLException;

/**
 * A hybrid {@link OVCTableHandle} that primarily uses HyperSQL tables, except
 * for the queue table, which uses a Memory table.  The stream table still uses
 * HyperSQL for now.
 */
public class HyperSQLAndMemoryOVCTableHandle extends HyperSQLOVCTableHandle {

  @Inject
  public HyperSQLAndMemoryOVCTableHandle(
      @Named("HyperSQLOVCTableHandleJDBCString")String hyperSqlJDBCString)
          throws SQLException {
    super(hyperSqlJDBCString);
  }

  @Override
  protected OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    // If this is the queue table, use a memory table, otherwise hypersql
    if (Bytes.equals(tableName, SimpleOVCTableHandle.queueOVCTable)) {
      return new MemoryOVCTable(tableName);
    }
    return super.createNewTable(tableName);
  }

  @Override
  public String getName() {
    return "hSQL+memory";
  }
}
