package com.continuuity.data.engine;


public interface VersionedTableHandle {

  public VersionedTable getTable(byte [] tableName);

  public VersionedQueueTable getQueueTable(byte [] queueName);

}
