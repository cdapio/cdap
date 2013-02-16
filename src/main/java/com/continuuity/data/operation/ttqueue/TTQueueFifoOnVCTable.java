package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * Distributed Queue with FIFO semantics
 * See <pre>https://wiki.continuuity.com/display/ENG/Random+%28FIFO%29+Queues</pre> for design information
 */
public class TTQueueFifoOnVCTable extends TTQueueAbstractOnVCTable {
  // Row header names and flags
//  static final byte [] CONSUMER_READ_POINTER = {30};

  protected TTQueueFifoOnVCTable(VersionedColumnarTable table, byte[] queueName, TimestampOracle timeOracle, CConfiguration conf) {
    super(table, queueName, timeOracle, conf);
  }

  @Override
  protected long fetchNextEntryId(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer) throws OperationException {
    return this.table.incrementAtomicDirtily(
      makeRowName(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
      CONSUMER_READ_POINTER, consumer.getGroupSize());
    //return this.table.incrementAtomicDirtily(makeRowName(GROUP_READ_POINTER, consumer.getGroupId()), GROUP_READ_POINTER, 1);
  }
}
