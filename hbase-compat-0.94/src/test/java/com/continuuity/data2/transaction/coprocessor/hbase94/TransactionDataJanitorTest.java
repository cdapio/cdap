package com.continuuity.data2.transaction.coprocessor.hbase94;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCache;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.util.hbase.ConfigurationTable;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MockRegionServerServices;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests filtering of invalid transaction data by the {@link TransactionDataJanitor} coprocessor.
 */
public class TransactionDataJanitorTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionDataJanitorTest.class);

  private static HBaseTestingUtility testUtil;
  private static LongArrayList invalidSet = new LongArrayList(new long[]{1L, 3L, 5L, 7L});
  private static CConfiguration conf;
  private static String tableNamespace;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    Configuration hConf = testUtil.getConfiguration();
    testUtil.startMiniCluster();
    testUtil.getDFSCluster().waitClusterUp();
    conf = CConfiguration.create();
    conf.unset(Constants.CFG_HDFS_USER);
    tableNamespace = conf.get(DataSetAccessor.CFG_TABLE_PREFIX, DataSetAccessor.DEFAULT_TABLE_PREFIX);
    // make sure the configuration is available to coprocessors
    ConfigurationTable configTable = new ConfigurationTable(testUtil.getConfiguration());
    configTable.write(ConfigurationTable.Type.DEFAULT, conf);

    // write an initial transaction snapshot
    // the only important paramter is the invalid set
    TransactionSnapshot snapshot =
      TransactionSnapshot.copyFrom(System.currentTimeMillis(), 0, 0, 0, invalidSet,
                                   new TreeMap<Long, InMemoryTransactionManager.InProgressTx>(),
                                   new HashMap<Long, Set<ChangeId>>(), new TreeMap<Long, Set<ChangeId>>());
    HDFSTransactionStateStorage tmpStorage = new HDFSTransactionStateStorage(conf, hConf);
    tmpStorage.startAndWait();
    tmpStorage.writeSnapshot(snapshot);
    tmpStorage.stopAndWait();
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testDataJanitorRegionScanner() throws Exception {
    String tableName = tableNamespace + ".TestDataJanitorRegionScanner";
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor cfd = new HColumnDescriptor(familyBytes);
    cfd.setMaxVersions(10);
    htd.addFamily(cfd);
    htd.addCoprocessor(TransactionDataJanitor.class.getName());
    Path tablePath = new Path("/tmp/" + tableName);
    Path hlogPath = new Path("/tmp/hlog");
    Path oldPath = new Path("/tmp/.oldLogs");
    Configuration hConf = testUtil.getConfiguration();
    FileSystem fs = FileSystem.get(hConf);
    assertTrue(fs.mkdirs(tablePath));
    HLog hlog = new HLog(fs, hlogPath, oldPath, hConf);
    HRegion region = new HRegion(tablePath, hlog, fs, hConf,
        new HRegionInfo(Bytes.toBytes(tableName)), htd, new MockRegionServerServices());
    try {
      region.initialize();
      TransactionStateCache cache = TransactionStateCache.get(hConf, tableNamespace);
      LOG.info("Coprocessor is using transaction state: " + cache.getLatestState());

      // Populate data, with timestamps 1-8. Odd are invalid, even are valid, oldest in use readPointer is 4 (i.e.
      // others prior to this can be removed).
      // So, on the cleanup we should see only even versions, but only one <= 4.
      for (int i = 1; i <= 8; i++) {
        for (int k = 1; k <= i; k++) {
          Put p = new Put(Bytes.toBytes(i));
          p.add(familyBytes, columnBytes, (long) k, Bytes.toBytes(k));
          region.put(p);
        }
      }

      List<KeyValue> results = Lists.newArrayList();

      // use the custom scanner to filter out results with timestamps in the invalid set
      Scan scan = new Scan();
      scan.setMaxVersions(10);
      TransactionDataJanitor.DataJanitorRegionScanner scanner =
          new TransactionDataJanitor.DataJanitorRegionScanner(4, invalidSet, region.getScanner(scan),
                                                              region.getRegionName());
      results.clear();
      // row "1" should be empty
      assertTrue(scanner.next(results));
      assertEquals(0, results.size());
      // first returned value should be "2" with version "2"
      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 2, new long[] {2L});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 3, new long[] {2L});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 4, new long[] {4L});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 5, new long[] {4L});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 6, new long[] {6L, 4L});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 7, new long[] {6L, 4L});

      results.clear();
      assertFalse(scanner.next(results));
      assertKeyValueMatches(results, 8, new long[] {8L, 6L, 4L});

      // force a flush to clear the data
      // during flush, the coprocessor should drop all KeyValues with timestamps in the invalid set
      LOG.info("Flushing region " + region.getRegionNameAsString());
      region.flushcache();

      // now a normal scan should only return the valid rows
      scan = new Scan();
      scan.setTimeRange(5L, 10L);
      RegionScanner regionScanner = region.getScanner(scan);
      results.clear();
      // first should be "6"
      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 6, new long[] {6L});
      // next should be "7"
      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 7, new long[] {6L});
      // final should be "8"
      results.clear();
      assertFalse(regionScanner.next(results));
      assertKeyValueMatches(results, 8, new long[] {8L});
    } finally {
      region.close();
    }
  }

  private void assertKeyValueMatches(List<KeyValue> results, int index, long[] versions) {
    assertEquals(versions.length, results.size());
    for (int i = 0; i < versions.length; i++) {
      KeyValue kv = results.get(i);
      assertArrayEquals(Bytes.toBytes(index), kv.getRow());
      assertEquals(versions[i], kv.getTimestamp());
      assertArrayEquals(Bytes.toBytes((int) versions[i]), kv.getValue());
    }
  }

  @Test
  public void testTransactionStateCache() throws Exception {
    TransactionStateCache cache = new TransactionStateCache(testUtil.getConfiguration(), tableNamespace);
    cache.startAndWait();
    // verify that the transaction snapshot read matches what we wrote in setupBeforeClass()
    TransactionSnapshot cachedSnapshot = cache.getLatestState();
    assertNotNull(cachedSnapshot);
    assertEquals(invalidSet, cachedSnapshot.getInvalid());
    cache.stopAndWait();
  }
}
