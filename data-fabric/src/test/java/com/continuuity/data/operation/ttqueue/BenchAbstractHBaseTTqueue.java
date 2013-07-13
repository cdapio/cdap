package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;

/**
 *
 */
public abstract class BenchAbstractHBaseTTqueue extends BenchTTQueue {

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

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(BenchTTQueue.RANDOM.nextInt());
    return new TTQueueOnVCTable(
        handle.getTable(Bytes.toBytes("BenchTable" + rand)),
        Bytes.toBytes("BQN" + rand),
        TestTTQueue.oracle, conf);
  }

  // Configuration for hypersql bench
  private static final BenchConfig config = new BenchConfig();
  static {
    config.numJustEnqueues = 100;
    config.queueEntrySize = 10;
    config.numEnqueuesThenSyncDequeueAckFinalize = 100;
  }

  @Override
  protected BenchConfig getConfig() {
    return config;
  }

}
