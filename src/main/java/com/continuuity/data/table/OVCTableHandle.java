package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.stream.StreamTable;

/**
 * Interface for retrieving instances of OVCTables and TTQueueTables.
 */
public interface OVCTableHandle {

  public String getName();

  public OrderedVersionedColumnarTable getTable(byte [] tableName) throws OperationException;

  public TTQueueTable getQueueTable(byte [] queueTableName) throws OperationException;

  /**
   * Gets Stream table
   * @param streamTableName name of the stream
   * @return instance of {@code StreamTable}
   * @throws OperationException
   */
  public StreamTable getStreamTable(byte [] streamTableName) throws OperationException;

}
