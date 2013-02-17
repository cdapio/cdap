package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.VersionedColumnarTable;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

/**
 * A table of {@link com.continuuity.data.operation.ttqueue.TTQueue}s.  See that API for details.
 */
public class TTQueueTableFifoOnVCTable extends TTQueueAbstractTableOnVCTable {

  private final VersionedColumnarTable table;

  public TTQueueTableFifoOnVCTable(VersionedColumnarTable table, TimestampOracle timeOracle, CConfiguration conf) {
    super(timeOracle, conf);
    this.table = table;
  }

  @Override
  protected TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueAbstractOnVCTable(this.table, queueName, this.timeOracle, this.conf);
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
