package com.continuuity.data.table;

import com.continuuity.data.operation.ttqueue.TTQueueTable;

/**
 * Interface for retrieving instances of ColumnarTables and TTQueueTables.
 */
public interface ColumnarTableHandle {

  public ColumnarTable getTable(byte [] tableName);

  public TTQueueTable getQueueTable(byte [] queueName);

}
