package com.continuuity.data.engine;

import com.continuuity.data.table.VersionedTable;

public interface VersionedTableHandle {

  public VersionedTable getTable(byte [] tableName);

  public VersionedQueueTable getQueueTable(byte [] queueName);

}
