package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Properties;

public class BenchHyperSQLTTqueue extends BenchTTQueue {

  private static final Properties hsqlProperties = new Properties();

//  private static final String hsql = "jdbc:hsqldb:file:/db/benchdb";
  private static final String hsql = "jdbc:hsqldb:mem:membenchdb";

  private static final DataFabricLocalModule module =
      new DataFabricLocalModule(hsql, hsqlProperties);

  private static final Injector injector = Guice.createInjector(module);

  private static final OVCTableHandle handle =
      injector.getInstance(OVCTableHandle.class);

  // Configuration for hypersql
  static {
    // Assume 1K rows and 512MB cache size
    hsqlProperties.setProperty("hsqldb.cache_rows", "" + 512000);
    hsqlProperties.setProperty("hsqldb.cache_size", "" + 512000);
    // Disable logging
    hsqlProperties.setProperty("hsqldb.log_data", "false");
  }

  // Configuration for hypersql bench
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
