package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;

/**
 * Interface for retrieving instances of OVCTables and TTQueueTables.
 */
public interface OVCTableHandle {

  public String getName();

  public OrderedVersionedColumnarTable getTable(byte [] tableName) throws OperationException;
}
