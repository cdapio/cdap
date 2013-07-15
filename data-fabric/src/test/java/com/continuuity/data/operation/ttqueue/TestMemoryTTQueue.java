package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;

/**
 *
 */
public class TestMemoryTTQueue extends TestTTQueue {

  @Override
  protected TTQueue createQueue(CConfiguration conf) {
    updateCConfiguration(conf);
    return new TTQueueOnVCTable(
      new MemoryOVCTable(Bytes.toBytes("TestMemoryTTQueue")),
      Bytes.toBytes("TestTTQueue"),
      TestTTQueue.oracle, conf);
  }

  @Override @Ignore
  public void testInjection() throws OperationException {
    // this test case uses MemoryOvcTable directly - no need to test it
  }

  @Override
  protected int getNumIterations() {
    return 2465;
  }
}
