package com.continuuity.data.operation.ttqueue;

import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestLevelDBTTQueue extends TestTTQueue {

  private static final Injector injector = Guice.createInjector (
      new DataFabricLevelDBModule());

  private static final OVCTableHandle handle =
      injector.getInstance(OVCTableHandle.class);

  private static final Random r = new Random();

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(r.nextInt());
    return new TTQueueOnVCTable(
        handle.getTable(Bytes.toBytes("TestMemoryTTQueueTable" + rand)),
        Bytes.toBytes("TestTTQueueName" + rand),
        TestTTQueue.timeOracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 100;
  }
}
