package com.continuuity.data.table;

import com.continuuity.data.operation.ttqueue.TTQueueTable;

/**
 * Interface for retrieving instances of OVCTables and TTQueueTables.
 */
public interface OVCTableHandle {

  public OrderedVersionedColumnarTable getTable(byte [] tableName);

  public TTQueueTable getQueueTable(byte [] queueTableName);

  public TTQueueTable getStreamTable(byte [] streamTableName);

}
