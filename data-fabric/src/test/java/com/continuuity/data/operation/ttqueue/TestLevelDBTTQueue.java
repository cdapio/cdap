package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBAndMemoryOVCTableHandle;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestLevelDBTTQueue extends TestTTQueue {

  private static CConfiguration conf;

  static {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    TestLevelDBTTQueue.conf = conf;
  }
  private static final Injector injector = Guice.createInjector (
      new DataFabricLevelDBModule(conf));

  private static final OVCTableHandle handle =
      injector.getInstance(OVCTableHandle.class);

  private static final Random r = new Random();

  @Override
  public void testInjection() {
    assertTrue(handle instanceof LevelDBAndMemoryOVCTableHandle);
  }

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(r.nextInt());
    updateCConfiguration(conf);
    return new TTQueueOnVCTable(
        handle.getTable(Bytes.toBytes("TestLevelDBTTQueueTable" + rand)),
        Bytes.toBytes("TestTTQueueName" + rand),
        TestTTQueue.oracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 1002;
  }
}
