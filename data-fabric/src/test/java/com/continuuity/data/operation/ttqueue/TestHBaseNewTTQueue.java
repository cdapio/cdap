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

import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestHBaseNewTTQueue extends TestTTQueueNew {

  private static Injector injector;

  private static OVCTableHandle handle;

  @Override
  public void testInjection() {
    assertTrue(handle instanceof HBaseOVCTableHandle);
  }

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      CConfiguration conf = CConfiguration.create();
      conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, false);
      injector = Guice.createInjector(new DataFabricDistributedModule(HBaseTestBase.getConfiguration(), conf));
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
    updateCConfiguration(conf);

    return new TTQueueNewOnVCTable(
      handle.getTable(Bytes.toBytes("TTQueueNewOnVCTable" + rand)),
      Bytes.toBytes("TestTTQueueName" + rand),
      TestTTQueue.oracle, conf);

//    return new TTQueueNewOnVCTable(
//      new MemoryOVCTable(Bytes.toBytes("TestMemoryNewTTQueue")),
//      Bytes.toBytes("TestTTQueue"),
//      TestTTQueue.oracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 100;
  }

}
