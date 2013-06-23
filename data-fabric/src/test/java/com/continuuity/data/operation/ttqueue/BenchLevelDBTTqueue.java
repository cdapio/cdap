package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;

public class BenchLevelDBTTqueue extends BenchTTQueue {

  private static CConfiguration conf;

  static {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    BenchLevelDBTTqueue.conf = conf;
  }
  private static final DataFabricLevelDBModule module =
      new DataFabricLevelDBModule(conf);

  private static final Injector injector = Guice.createInjector(module);

  private static final OVCTableHandle handle =
      injector.getInstance(OVCTableHandle.class);

  // Configuration for leveldb
  static {
    // TODO: See if any leveldb knobs are worth configuring
  }

  // Configuration for leveldb bench
  private static final BenchConfig config = new BenchConfig();
  static {
    config.numJustEnqueues = 100;
    config.queueEntrySize = 10;
    config.numEnqueuesThenSyncDequeueAckFinalize = 100;
  }

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(BenchTTQueue.r.nextInt());
    return new TTQueueOnVCTable(
        handle.getTable(Bytes.toBytes("BenchTable" + rand)),
        Bytes.toBytes("BQN" + rand),
        TestTTQueue.oracle, conf);
  }

  @Override
  protected BenchConfig getConfig() {
    return config;
  }

}
