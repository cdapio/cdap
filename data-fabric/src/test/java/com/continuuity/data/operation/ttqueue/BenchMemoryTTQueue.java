package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class BenchMemoryTTQueue extends BenchTTQueue {

  // Configuration of memory ttqueue bench
  private static final BenchConfig config = new BenchConfig();
  static {
    config.numJustEnqueues = 10000;
    config.queueEntrySize = 10;
    config.numEnqueuesThenSyncDequeueAckFinalize = 10000;
  }
  
  @Override
  protected TTQueue createQueue(CConfiguration conf) {
    return new TTQueueOnVCTable(
        new MemoryOVCTable(Bytes.toBytes("TestMemoryTTQueue")),
        Bytes.toBytes("TestTTQueue"),
        TestTTQueue.oracle, conf);
  }

  @Override
  protected BenchConfig getConfig() {
    return config;
  }
}
