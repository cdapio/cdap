package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.TTQueueTable;

/**
 * Interface for retrieving instances of ColumnarTables and TTQueueTables.
 */
public interface ColumnarTableHandle {

  public ColumnarTable getTable(byte [] tableName) throws OperationException;

  public TTQueueTable getQueueTable(byte [] queueName);

}
