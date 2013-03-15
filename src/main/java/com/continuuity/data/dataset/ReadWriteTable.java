package com.continuuity.data.dataset;


import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.data.DataFabric;
import com.continuuity.data.operation.executor.TransactionProxy;

/**
 * The read/write runtime implementation of the Table data set.
 */
public class ReadWriteTable extends RuntimeTable {

  /**
   * Given a Table, create a new ReadWriteTable and make it the delegate for that
   * table.
   *
   * @param table the original table
   * @param fabric the data fabric
   * @param metricName the name to use for emitting metrics
   * @param proxy transaction proxy for all operations
   * @return the new ReadWriteTable
   */
  public static ReadWriteTable setReadWriteTable(Table table, DataFabric fabric,
                                                 String metricName, TransactionProxy proxy) {
    ReadWriteTable readWriteTable = new ReadWriteTable(table, fabric, proxy);
    readWriteTable.setMetricName(metricName);
    table.setDelegate(readWriteTable);
    return readWriteTable;
  }

  /**
   * private constructor, only to be called from @see #setReadWriteTable().
   * @param table the original table
   * @param fabric the data fabric
   * @param proxy transaction proxy for all operations
   */
  private ReadWriteTable(Table table, DataFabric fabric, TransactionProxy proxy) {
    super(table, fabric, proxy);
  }
}
