package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableRoundRobinOnVCTable extends TTQueueAbstractTableOnVCTable {

  private final VersionedColumnarTable table;

  public TTQueueTableRoundRobinOnVCTable(VersionedColumnarTable table, TimestampOracle timeOracle, CConfiguration conf) {
    super(timeOracle, conf);
    this.table = table;
  }

  @Override
  protected TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueRoundRobinOnVCTable(this.table, queueName, this.timeOracle, this.conf);
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
