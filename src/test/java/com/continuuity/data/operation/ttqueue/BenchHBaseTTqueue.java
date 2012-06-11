package com.continuuity.data.operation.ttqueue;

import org.junit.Ignore;

@Ignore
public abstract class BenchHBaseTTqueue extends BenchTTQueue {
//
//  private static final HBaseTestingUtility hbTestUtil =
//      new HBaseTestingUtility();
//
//  private static MiniHBaseCluster miniCluster;
//
//  private static final Configuration conf = hbTestUtil.getConfiguration();
//  
//  private static final Injector injector =
//      Guice.createInjector(new DataFabricDistributedModule(conf));
//
//  private static final OVCTableHandle handle =
//      injector.getInstance(OVCTableHandle.class);
//
//  // Configuration for hypersql bench
//  private static final BenchConfig config = new BenchConfig();
//  static {
//    config.numJustEnqueues = 1000;
//    config.queueEntrySize = 10;
//    config.numEnqueuesThenSyncDequeueAckFinalize = 1000;
//  }
//
//  @BeforeClass @Ignore
//  public static void startEmbeddedHBase() {
//    try {
//      miniCluster = hbTestUtil.startMiniCluster(1, 1);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  @AfterClass @Ignore
//  public static void stopEmbeddedHBase() {
//    try {
//      if (miniCluster != null) miniCluster.shutdown();
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//  
//  @Override
//  protected TTQueue createQueue(CConfiguration conf) {
//    String rand = "" + Math.abs(BenchTTQueue.r.nextInt());
//    return new TTQueueOnVCTable(
//        handle.getTable(Bytes.toBytes("BenchTable" + rand)),
//        Bytes.toBytes("BQN" + rand),
//        TestTTQueue.timeOracle, conf);
//  }
//
//  @Override
//  protected BenchConfig getConfig() {
//    return config;
//  }

}
