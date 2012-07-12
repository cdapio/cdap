package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;

public class TestMemoryTTQueue extends TestTTQueue {

  @Override
  protected TTQueue createQueue(CConfiguration conf) {
    return new TTQueueOnVCTable(
        new MemoryOVCTable(Bytes.toBytes("TestMemoryTTQueue")),
        Bytes.toBytes("TestTTQueue"),
        TestTTQueue.timeOracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 250;
  }
}
