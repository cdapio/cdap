package com.continuuity.data.dataset;

import com.continuuity.api.data.Closure;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.ReadColumnRange;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.WriteOperation;

import java.util.Map;

/**
 * The read-only runtime implementation of the Table data set.
 */
public class ReadOnlyTable extends RuntimeTable {

  /**
   * Given a Table, create a new ReadOnlyTable and make it the delegate for that
   * table.
   *
   * @param table the original table
   * @param fabric the data fabric
   * @return the new ReadOnlyTable
   */
  public static ReadOnlyTable setReadOnlyTable(Table table, DataFabric fabric) {
    ReadOnlyTable readOnlyTable = new ReadOnlyTable(table, fabric);
    table.setDelegate(readOnlyTable);
    return readOnlyTable;
  }

  /**
   * package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor
   * @param table the original table
   * @param fabric the data fabric
   */
  ReadOnlyTable(Table table, DataFabric fabric) {
    super(table, fabric);
  }

  // synchronous read
  @Override
  public OperationResult<Map<byte[], byte[]>> read(Read read)
    throws OperationException {
    if (read.getColumns() != null) {
      return this.getDataFabric().read(new com.continuuity.api.data.Read(this.tableName(), read.getRow(),
                                                                         read.getColumns()));
    } else {
      return this.getDataFabric().read(new ReadColumnRange(this.tableName(), read.getRow(), read.getStartCol(),
                                                           read.getStopCol()));
    }
  }

  // no support for write operations
  @Override
  public void stage(WriteOperation op) {
    throw new UnsupportedOperationException("Write operations are not supported by read only table.");
  }

  // no support for write operations
  @Override
  public void exec(WriteOperation op) throws OperationException {
    throw new UnsupportedOperationException("Write operations are not supported by read only table.");
  }

  // no support for write operations, including the increment closure
  @Override
  public Closure closure(Increment increment) {
    throw new UnsupportedOperationException("Write operations are not supported by read only table.");
  }

}

