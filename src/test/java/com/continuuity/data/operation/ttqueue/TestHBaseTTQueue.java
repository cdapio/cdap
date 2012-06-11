package com.continuuity.data.operation.ttqueue;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;

@Ignore
public class TestHBaseTTQueue extends TestTTQueue {

  private static HBaseTestingUtility hbTestUtil;

  private static MiniHBaseCluster miniCluster;

  private static Configuration conf;
  
  private static Injector injector;     

  private static OVCTableHandle handle;

  private static final Random r = new Random();

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      System.out.println("STARTING UP");
      Thread.sleep(2000);
      hbTestUtil = new HBaseTestingUtility();
      conf = hbTestUtil.getConfiguration();
      System.out.println("\n\n\n\n\nConf: " + conf.toString());
      System.out.println("\n\n\n\n\nConf: " + conf.get("hbase.zookeeper.quorum"));
      conf.set("hbase.zookeeper.quorum", "127.0.0.1");
      Thread.sleep(2000);
      injector = Guice.createInjector(new DataFabricDistributedModule(conf));
      handle = injector.getInstance(OVCTableHandle.class);
      hbTestUtil.startMiniZKCluster(1);
      System.out.println("\n\n\n\nSLEEPING\n\n\n\n");
      Thread.sleep(10000);
//      miniCluster = hbTestUtil.startMiniCluster(1, 1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void stopEmbeddedHBase() {
    try {
      if (miniCluster != null) miniCluster.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected TTQueue createQueue(CConfiguration conf) {
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
