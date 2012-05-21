package com.continuuity.data.engine;

public interface SimpleTableHandle {

  public SimpleTable getTable(byte [] tableName);

  public SimpleQueueTable getQueueTable(byte [] queueName);

}
