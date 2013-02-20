package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class BenchLevelDBTTqueue extends BenchTTQueue {

  private static final DataFabricLevelDBModule module =
      new DataFabricLevelDBModule();

  private static final Injector injector = Guice.createInjector(module);

  private static final OVCTableHandle handle =
      injector.getInstance(OVCTableHandle.class);

  // Configuration for leveldb
  static {
    // TODO: See if any leveldb knobs are worth configuring
  }

  // Configuration for hypersql bench
  private static final BenchConfig config = new BenchConfig();
  static {
    config.numJustEnqueues = 4000;
    config.queueEntrySize = 10;
    config.numEnqueuesThenSyncDequeueAckFinalize = 4000;
  }

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(BenchTTQueue.r.nextInt());
    return new TTQueueOnVCTable(
        handle.getTable(Bytes.toBytes("BenchTable" + rand)),
        Bytes.toBytes("BQN" + rand),
        TestTTQueue.timeOracle, conf);
  }

  @Override
  protected BenchConfig getConfig() {
    return config;
  }

}
