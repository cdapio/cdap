package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.hbase.ttqueue.HBQConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class BenchNativeHBaseTTqueue extends BenchTTQueue {

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static HTable createTable(byte[] tableName, byte[] family)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    htd.addFamily(hcd);
    HBaseTestBase.getHBaseAdmin().createTable(htd);
    return new HTable(HBaseTestBase.getConfiguration(), tableName);
  }

  @AfterClass
  public static void stopEmbeddedHBase() {
    try {
      HBaseTestBase.stopHBase();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected TTQueue createQueue(CConfiguration conf) throws OperationException {
    String rand = "" + Math.abs(BenchTTQueue.r.nextInt());
    HTable table;
    try {
      table = createTable(Bytes.toBytes("BenchNativeHBaseQueueTTQ" + rand),
          HBQConstants.HBQ_FAMILY);
    } catch (IOException e) {
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
    return new TTQueueOnHBaseNative(table,
        Bytes.toBytes("BenchTTQueueName" + rand), TestTTQueue.oracle, conf);
  }

  // Configuration for hypersql bench
  private static final BenchConfig config = new BenchConfig();
  static {
    config.numJustEnqueues = 4000;
    config.queueEntrySize = 10;
    config.numEnqueuesThenSyncDequeueAckFinalize = 4000;
  }

  @Override
  protected BenchConfig getConfig() {
    return config;
  }

}
