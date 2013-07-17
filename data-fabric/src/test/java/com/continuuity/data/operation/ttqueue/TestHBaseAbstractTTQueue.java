package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;

import java.util.Random;

/**
 *
 */
public abstract class TestHBaseAbstractTTQueue extends TestTTQueue {

  protected static Injector injector;
  protected static OVCTableHandle handle;

  @AfterClass
  public static void stopEmbeddedHBase() {
    try {
      HBaseTestBase.stopHBase();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final Random r = new Random();

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(r.nextInt());
    updateCConfiguration(conf);

    return new TTQueueOnVCTable(
      handle.getTable(Bytes.toBytes("TTQueueOnVCTable" + rand)),
      Bytes.toBytes("TestTTQueueName" + rand),
      TestTTQueue.oracle, conf);

//    return new TTQueueOnVCTable(
//      new MemoryOVCTable(Bytes.toBytes("TestMemoryTTQueue")),
//      Bytes.toBytes("TestTTQueue"),
//      TestTTQueue.oracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 100;
  }

}
