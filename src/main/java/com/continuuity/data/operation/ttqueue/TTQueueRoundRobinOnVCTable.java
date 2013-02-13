package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 *
 */
public class TTQueueRoundRobinOnVCTable extends TTQueueAbstractOnVCTable {

  protected TTQueueRoundRobinOnVCTable(VersionedColumnarTable table, byte[] queueName, TimestampOracle timeOracle, CConfiguration conf) {
    super(table, queueName, timeOracle, conf);
  }

  @Override
  protected long fetchNextEntryId(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer) {
    return -1;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
