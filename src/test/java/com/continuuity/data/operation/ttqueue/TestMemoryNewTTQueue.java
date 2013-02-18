package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;

@Ignore
public class TestMemoryNewTTQueue extends TestTTQueue {

  @Override
  protected TTQueue createQueue(CConfiguration conf) {
    return new TTQueueNewOnVCTable(
      new MemoryOVCTable(Bytes.toBytes("TestMemoryNewTTQueue")),
      Bytes.toBytes("TestTTQueue"),
      TestTTQueue.timeOracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 250;
  }
}
