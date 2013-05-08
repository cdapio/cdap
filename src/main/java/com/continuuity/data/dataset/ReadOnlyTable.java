package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.WriteOperation;
import com.continuuity.data.DataFabric;
import com.continuuity.data.operation.executor.TransactionProxy;

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
   * @param metricName the name to use for emitting metrics
   * @param proxy transaction proxy for all operations
   * @return the new ReadOnlyTable
   */
  public static ReadOnlyTable setReadOnlyTable(Table table, DataFabric fabric,
                                               String metricName, TransactionProxy proxy) {
    ReadOnlyTable readOnlyTable = new ReadOnlyTable(table, fabric, proxy);
    readOnlyTable.setMetricName(metricName);
    table.setDelegate(readOnlyTable);
    return readOnlyTable;
  }

  /**
   * Package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor.
   * @param table the original table
   * @param proxy transaction proxy for all operations
   * @param fabric the data fabric
   */
  ReadOnlyTable(Table table, DataFabric fabric, TransactionProxy proxy) {
    super(table, fabric, proxy);
  }

  // no support for write operations
  @Override
  public void write(WriteOperation op) throws OperationException {
    throw new UnsupportedOperationException("Write operations are not supported by read only table.");
  }

  // no support for write operations, including the increment
  @Override
  public Map<byte[], Long> incrementAndGet(Increment increment) throws OperationException {
    throw new UnsupportedOperationException("Increment is not supported by read only table.");
  }

}

