package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableOnVCTable extends TTQueueAbstractTableOnVCTable {

  private final VersionedColumnarTable table;

  public TTQueueTableOnVCTable(VersionedColumnarTable table, TransactionOracle oracle, CConfiguration conf) {
    super(oracle, conf);
    this.table = table;
  }

  protected TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueOnVCTable(this.table, queueName, this.oracle,
        this.conf);
    TTQueue existing = this.queues.putIfAbsent(queueName, queue);
    return existing != null ? existing : queue;
  }

  @Override
  public void clear() throws OperationException {
    table.clear();
  }

}
