package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;

public class TestHBaseNewTTQueue extends TestTTQueue {

  private static Injector injector;

  private static OVCTableHandle handle;

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      injector = Guice.createInjector(
          new DataFabricDistributedModule(HBaseTestBase.getConfiguration()));
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
    return new TTQueueNewOnVCTable(
        handle.getTable(Bytes.toBytes("TTQueueNewOnVCTable" + rand)),
        Bytes.toBytes("TestTTQueueName" + rand),
        TestTTQueue.timeOracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 100;
  }

  // Tests that do not work on HBaseNewTTQueue

  /**
   * Currently not working.  Will be fixed in ENG-???.
   */
//  @Override @Test @Ignore
//  public void testEvictOnAck_OneGroup() {}
//
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

//  @Override @Test @Ignore
//  public void testMultipleConsumerMultiTimeouts() {}
//
//  @Override @Test @Ignore
//  public void testMultiConsumerMultiGroup() {}
//
//  @Override @Test @Ignore
//  public void testEvictOnAck_ThreeGroups() {}
}
