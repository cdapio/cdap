package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.TTQueueTable;

/**
 * Interface for retrieving instances of OVCTables and TTQueueTables.
 */
public interface OVCTableHandle {

  public String getName();

  public OrderedVersionedColumnarTable getTable(byte [] tableName) throws OperationException;

  public TTQueueTable getQueueTable(byte [] queueTableName) throws OperationException;

  public TTQueueTable getStreamTable(byte [] streamTableName) throws OperationException;

}
