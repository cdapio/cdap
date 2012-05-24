package com.continuuity.data.engine;

import com.continuuity.data.table.SimpleTable;

public interface SimpleTableHandle {

  public SimpleTable getTable(byte [] tableName);

  public SimpleQueueTable getQueueTable(byte [] queueName);

}
