package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;
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


  @Override @Test @Ignore
  public void testEvictOnAck_OneGroup() throws Exception { }

  @Override @Test @Ignore
  public void testSingleConsumerSingleEntryWithInvalid_Empty_ChangeSizeAndToMulti() {}

  @Override @Test @Ignore
  public void testSingleConsumerMultiEntry_Empty_ChangeToSingleConsumerSingleEntry() {}

  @Override @Test @Ignore
  public void testSingleConsumerSingleGroup_dynamicReconfig() {}

  @Override @Test @Ignore
  public void testLotsOfAsyncDequeueing() {}

  @Override @Test @Ignore
  public void testMultiConsumerSingleGroup_dynamicReconfig() {}

  @Override @Test @Ignore
  public void testSingleConsumerMulti() {}

  @Override @Test @Ignore
  public void testMultipleConsumerMultiTimeouts() {}

//  @Override @Test @Ignore
//  public void testMultiConsumerMultiGroup() {}

  @Override @Test @Ignore
  public void testEvictOnAck_ThreeGroups() {}
}
