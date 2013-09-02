package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
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

/**
 * test the queues with HBase.
 */
@Ignore
public class TestHBaseTTQueue extends TestHBaseAbstractTTQueue {

  protected static Injector injector;
  protected static OVCTableHandle handle;

  @Override
  public void testInjection() {
    assertTrue(handle instanceof HBaseOVCTableHandle);
  }

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      DataFabricDistributedModule module = new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
      injector = Guice.createInjector(module,
                                      new ConfigModule(module.getConfiguration(), HBaseTestBase.getConfiguration()),
                                      new LocationRuntimeModule().getInMemoryModules());

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

    return new TTQueueOnVCTable(
      handle.getTable(Bytes.toBytes("TTQueueOnVCTable" + rand)),
      Bytes.toBytes("TestTTQueueName" + rand),
      TestTTQueue.oracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 100;
  }
}
