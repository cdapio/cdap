package com.continuuity.data2.transaction.coprocessor.hbase96;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCache;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCacheSupplier;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;
import com.continuuity.test.SlowTests;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests filtering of invalid transaction data by the {@link TransactionDataJanitor} coprocessor.
 */
@Category(SlowTests.class)
public class TransactionDataJanitorTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionDataJanitorTest.class);

  // 8 versions, 1 hour apart, latest is current ts.
  private static final long[] V;

  static {
    long now = System.currentTimeMillis();
    V = new long[9];
    for (int i = 0; i < V.length; i++) {
      V[i] = (now - TimeUnit.HOURS.toMillis(9 - i)) * TxConstants.MAX_TX_PER_MS;
    }
  }

  private static HBaseTestingUtility testUtil;
  private static LongArrayList invalidSet = new LongArrayList(new long[]{V[3], V[5], V[7]});

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    Configuration hConf = testUtil.getConfiguration();
    testUtil.startMiniCluster();
    testUtil.getDFSCluster().waitClusterUp();
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_HDFS_USER);
    conf.unset(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);

    // write an initial transaction snapshot
    TransactionSnapshot snapshot =
      TransactionSnapshot.copyFrom(
        System.currentTimeMillis(), V[6], V[7], invalidSet,
        // this will set visibility upper bound to V[6]
        Maps.newTreeMap(ImmutableSortedMap.of(5L, new InMemoryTransactionManager.InProgressTx(V[6], Long.MAX_VALUE))),
        new HashMap<Long, Set<ChangeId>>(), new TreeMap<Long, Set<ChangeId>>());
    HDFSTransactionStateStorage tmpStorage =
      new HDFSTransactionStateStorage(conf, hConf, new SnapshotCodecProvider(conf));
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
    String tableName = "TestRegionScanner";
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor cfd = new HColumnDescriptor(familyBytes);
    // with that, all older than upper visibility bound by 3 hours should be expired by TTL logic
    cfd.setValue(TxConstants.PROPERTY_TTL, String.valueOf(TimeUnit.HOURS.toMillis(3)));
    cfd.setMaxVersions(10);
    htd.addFamily(cfd);
    htd.addCoprocessor(TransactionDataJanitor.class.getName());
    Path tablePath = new Path("/tmp/" + tableName);
    Path hlogPath = new Path("/tmp/hlog");
    Configuration hConf = testUtil.getConfiguration();
    FileSystem fs = FileSystem.get(hConf);
    assertTrue(fs.mkdirs(tablePath));
    HLog hLog = HLogFactory.createHLog(fs, hlogPath, "testRegionScanner", hConf);
    HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf(tableName));
    HRegionFileSystem regionFS = HRegionFileSystem.createRegionOnFileSystem(hConf, fs, tablePath, regionInfo);
    HRegion region = new HRegion(regionFS, hLog, hConf, htd,
                                 new MockRegionServerServices(hConf, testUtil.getZooKeeperWatcher(testUtil)));
    try {
      region.initialize();
      TransactionStateCache cache = new TransactionStateCacheSupplier(hConf).get();
      LOG.info("Coprocessor is using transaction state: " + cache.getLatestState());

      for (int i = 1; i <= 8; i++) {
        for (int k = 1; k <= i; k++) {
          Put p = new Put(Bytes.toBytes(i));
          p.add(familyBytes, columnBytes, V[k], Bytes.toBytes(V[k]));
          region.put(p);
        }
      }

      List<Cell> results = Lists.newArrayList();

      // use the custom scanner to filter out results with timestamps in the invalid set
      Scan scan = new Scan();
      scan.setMaxVersions(10);
      // NOTE: v1 and v2 are expired based on ttl: they are older than visibilityUpperBound - 3 hour
      //       v3, v5, v7 are invalid
      TransactionDataJanitor.DataJanitorRegionScanner scanner =
        new TransactionDataJanitor.DataJanitorRegionScanner(V[6], V[3], invalidSet, region.getScanner(scan),
                                                            region.getRegionName());

      // first returned value should be "4" with version "4"
      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 4, new long[] {V[4]});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 5, new long[] {V[4]});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 6, new long[] {V[6]});

      results.clear();
      assertTrue(scanner.next(results));
      assertKeyValueMatches(results, 7, new long[] {V[6]});

      results.clear();
      assertFalse(scanner.next(results));
      assertKeyValueMatches(results, 8, new long[] {V[8], V[6]});

      // force a flush to clear the data
      // during flush, the coprocessor should drop all KeyValues with timestamps in the invalid set
      LOG.info("Flushing region " + region.getRegionNameAsString());
      region.flushcache();

      // now a normal scan should only return the valid rows - testing that cleanup works on flush
      scan = new Scan();
      scan.setMaxVersions(10);
      RegionScanner regionScanner = region.getScanner(scan);

      // first returned value should be "4" with version "4"
      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 4, new long[] {V[4]});

      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 5, new long[] {V[4]});

      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 6, new long[] {V[6]});

      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 7, new long[] {V[6]});

      results.clear();
      assertFalse(regionScanner.next(results));
      assertKeyValueMatches(results, 8, new long[] {V[8], V[6]});
    } finally {
      region.close();
    }
  }

  private void assertKeyValueMatches(List<Cell> results, int index, long[] versions) {
    assertEquals(versions.length, results.size());
    for (int i = 0; i < versions.length; i++) {
      Cell kv = results.get(i);
      assertArrayEquals(Bytes.toBytes(index), kv.getRow());
      assertEquals(versions[i], kv.getTimestamp());
      assertArrayEquals(Bytes.toBytes(versions[i]), kv.getValue());
    }
  }

  @Test
  public void testTransactionStateCache() throws Exception {
    TransactionStateCache cache = new TransactionStateCache();
    cache.setConf(testUtil.getConfiguration());
    cache.startAndWait();
    // verify that the transaction snapshot read matches what we wrote in setupBeforeClass()
    TransactionSnapshot cachedSnapshot = cache.getLatestState();
    assertNotNull(cachedSnapshot);
    assertEquals(invalidSet, cachedSnapshot.getInvalid());
    cache.stopAndWait();
  }

  private static class MockRegionServerServices implements RegionServerServices {
    private final Configuration hConf;
    private final ZooKeeperWatcher zookeeper;
    private final Map<String, HRegion> regions = new HashMap<String, HRegion>();
    private boolean stopping = false;
    private final ConcurrentSkipListMap<byte[], Boolean> rit =
      new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);
    private HFileSystem hfs = null;
    private ServerName serverName = null;
    private RpcServerInterface rpcServer = null;
    private volatile boolean abortRequested;


    public MockRegionServerServices(Configuration hConf, ZooKeeperWatcher zookeeper) {
      this.hConf = hConf;
      this.zookeeper = zookeeper;
    }

    @Override
    public boolean isStopping() {
      return stopping;
    }

    @Override
    public HLog getWAL(HRegionInfo regionInfo) throws IOException {
      return null;
    }

    @Override
    public CompactionRequestor getCompactionRequester() {
      return null;
    }

    @Override
    public FlushRequester getFlushRequester() {
      return null;
    }

    @Override
    public RegionServerAccounting getRegionServerAccounting() {
      return null;
    }

    @Override
    public TableLockManager getTableLockManager() {
      return new TableLockManager.NullTableLockManager();
    }

    @Override
    public void postOpenDeployTasks(HRegion r, CatalogTracker ct) throws KeeperException, IOException {
    }

    @Override
    public RpcServerInterface getRpcServer() {
      return rpcServer;
    }

    @Override
    public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
      return rit;
    }

    @Override
    public FileSystem getFileSystem() {
      return hfs;
    }

    @Override
    public Leases getLeases() {
      return null;
    }

    @Override
    public ExecutorService getExecutorService() {
      return null;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return null;
    }

    @Override
    public Map<String, HRegion> getRecoveringRegions() {
      return null;
    }

    @Override
    public void updateRegionFavoredNodesMapping(String encodedRegionName, List<HBaseProtos.ServerName> favoredNodes) {
    }

    @Override
    public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
      return new InetSocketAddress[0];
    }

    @Override
    public void addToOnlineRegions(HRegion r) {
      regions.put(r.getRegionNameAsString(), r);
    }

    @Override
    public boolean removeFromOnlineRegions(HRegion r, ServerName destination) {
      return regions.remove(r.getRegionInfo().getEncodedName()) != null;
    }

    @Override
    public HRegion getFromOnlineRegions(String encodedRegionName) {
      return regions.get(encodedRegionName);
    }

    @Override
    public List<HRegion> getOnlineRegions(TableName tableName) throws IOException {
      return null;
    }

    @Override
    public Configuration getConfiguration() {
      return hConf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return zookeeper;
    }

    @Override
    public ServerName getServerName() {
      return serverName;
    }

    @Override
    public void abort(String why, Throwable e) {
      this.abortRequested = true;
    }

    @Override
    public boolean isAborted() {
      return abortRequested;
    }

    @Override
    public void stop(String why) {
      this.stopping = true;
    }

    @Override
    public boolean isStopped() {
      return stopping;
    }
  }
}
