package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
<<<<<<< HEAD
import com.continuuity.data.table.VersionedColumnarTable;
=======
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentSkipListMap;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;
>>>>>>> master

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableOnVCTable extends TTQueueAbstractTableOnVCTable {

  private final VersionedColumnarTable table;

  public TTQueueTableOnVCTable(VersionedColumnarTable table, TimestampOracle timeOracle, CConfiguration conf) {
    super(timeOracle,conf);
    this.table = table;
  }

  protected TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueOnVCTable(this.table, queueName, this.timeOracle,
        this.conf);
    TTQueue existing = this.queues.putIfAbsent(queueName, queue);
    return existing != null ? existing : queue;
  }

  @Override
  public void clear() throws OperationException {
    table.clear();
  }
}
