package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.Random;

import static org.junit.Assert.assertTrue;

// Ignoring all TestHBaseTTQueue tests for now. New queue algorithm has replaced this class. This class will be
// removed later.
@Ignore
public class TestHBaseTTQueue extends TestTTQueue {

  private static Injector injector;

  private static OVCTableHandle handle;

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      CConfiguration conf = CConfiguration.create();
      conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, false);
      injector = Guice.createInjector(
          new DataFabricDistributedModule(HBaseTestBase.getConfiguration(), conf));
      handle = injector.getInstance(OVCTableHandle.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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
    return new TTQueueOnVCTable(
        handle.getTable(Bytes.toBytes("TestMemoryTTQueueTable" + rand)),
        Bytes.toBytes("TestTTQueueName" + rand),
        TestTTQueue.oracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 100;
  }

  @Override
  public void testInjection() {
    assertTrue(handle instanceof HBaseOVCTableHandle);
  }

  // This test fails when native queues are disabled. Since this implementation will no longer be used it is being
  // disabled.
  @Override @Ignore
  public void testEvictOnAck_ThreeGroups() throws Exception {
  }
}
