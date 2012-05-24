package com.continuuity.data.engine;

public interface VersionedTableHandle extends SimpleTableHandle {

  public VersionedTable getTable(byte [] tableName);

  public VersionedQueueTable getQueueTable(byte [] queueName);

}
