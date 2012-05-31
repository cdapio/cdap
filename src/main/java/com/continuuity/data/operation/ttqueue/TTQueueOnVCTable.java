package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.table.VersionedColumnarTable;

/**
 * Implementation of a single {@link TTQueue} on a single
 * {@link VersionedColumnarTable}.
 */
//TODO implement
public abstract class TTQueueOnVCTable implements TTQueue {

  private final VersionedColumnarTable table;
  private final byte [] queueName;
  
  public TTQueueOnVCTable(final VersionedColumnarTable table,
      final byte [] queueName) {
    this.table = table;
    this.queueName = queueName;
  }

  VersionedColumnarTable getTable() {
    return table;
  }

  byte [] getQueueName() {
    return queueName;
  }

}
