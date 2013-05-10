package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * A table of {@link com.continuuity.data.operation.ttqueue.TTQueue}s.  See that API for details.
 */
public class TTQueueTableNewOnVCTable extends TTQueueAbstractTableOnVCTable {

  private final VersionedColumnarTable table;

  public TTQueueTableNewOnVCTable(VersionedColumnarTable table, TransactionOracle oracle, CConfiguration conf) {
    super(oracle, conf);
    this.table = table;
  }

  @Override
  protected TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueNewOnVCTable(this.table, queueName, this.oracle, this.conf);
    TTQueue existing = this.queues.putIfAbsent(queueName, queue);
    return existing != null ? existing : queue;
  }

  @Override
  public String getGroupInfo(byte[] queueName, int groupId)
      throws OperationException {
    // TODO: implement this :)
    return "GroupInfo not supported";
  }

  @Override
  public String getEntryInfo(byte[] queueName, long entryId)
      throws OperationException {
    // TODO: implement this :)
    return "EntryInfo not supported";
  }

  @Override
  public void clear() throws OperationException {
    table.clear();
  }
}
