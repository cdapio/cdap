package com.continuuity.data.hbase;

import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A base class that can be used to easily run a test within an embedded
 * HBase cluster (includes embedded ZooKeeper, HDFS, and HBase).
 *
 * To use, simply extend this class and create your tests like normal.  From
 * within your tests, you can access the underlying HBase cluster through
 * {@link #getConfiguration()}, {@link #getHBaseAdmin()}, and
 * {@link #getHTable(byte[])}.
 *
 * Alternatively, you can call the {@link #startHBase()} and {@link #stopHBase()}
 * methods directly from within your own BeforeClass/AfterClass methods.
 *
 * Note:  This test is somewhat heavy-weight and takes 10-20 seconds to startup.
 */
public abstract class HBaseTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTestBase.class);

  protected Configuration conf;

  public MiniZooKeeperCluster zkCluster;

  protected MiniDFSCluster dfsCluster;

  protected MiniHBaseCluster hbaseCluster;

  private final List<File> tmpDirList = Lists.newArrayList();

  // Accessors for test implementations

  public Configuration getConfiguration() {
    return conf;
  }

  public HBaseAdmin getHBaseAdmin() throws IOException {
    return new HBaseAdmin(conf);
  }

  public HTable getHTable(byte [] tableName) throws IOException {
    return new HTable(conf, tableName);
  }

  // Temporary directories

  private final Random r = new Random();

  public File getRandomTempDir() {
    File dir = Files.createTempDir();
    tmpDirList.add(dir);
    return dir;
  }

  public String getZkConnectionString() {
    return "localhost:" + zkCluster.getClientPort();
  }

  // Test startup / teardown

  @BeforeClass
  public void startHBase() throws Exception {
    conf = new Configuration();
    // Set any necessary configurations (disable UIs to prevent port conflicts)
    conf.setInt("hbase.regionserver.info.port", -1);
    conf.setInt("hbase.master.info.port", -1);
    // Disable compression since it may not be available in environment where we run unit-test
    conf.set(HBaseTableUtil.CFG_HBASE_TABLE_COMPRESSION, "NONE");

    // Start ZooKeeper

    zkCluster = new MiniZooKeeperCluster(conf);
    System.err.println("Starting ZK in 1 sec...");
    Thread.sleep(1000);
    int zkPort = zkCluster.startup(getRandomTempDir(), 1);
    System.err.println("\n\nStarted ZK on port " + zkPort + "\n\n\n");

    // Add ZK info to conf
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(zkPort));

    // Start DFS

    File dfsPath = getRandomTempDir();
    System.setProperty("test.build.data", dfsPath.toString());
    System.setProperty("test.cache.data", dfsPath.toString());
    System.err.println("Instantiating dfs cluster in 1 sec...");
    Thread.sleep(1000);
    dfsCluster = new MiniDFSCluster.Builder(conf)
        .nameNodePort(0)
        .numDataNodes(1)
        .format(true)
        .manageDataDfsDirs(true)
        .manageNameDfsDirs(true)
        .build();
    System.err.println("Waiting for dfs to start...");
    dfsCluster.waitClusterUp();
    System.err.println("DFS started...");
    Thread.sleep(1000);

    // Add HDFS info to conf
    conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString());

    // Start HBase
    createHBaseRootDir(conf);
    conf.setInt("hbase.master.wait.on.regionservers.mintostart", 1);
    conf.setInt("hbase.master.wait.on.regionservers.maxtostart", 1);
    conf.set("hbase.master.logcleaner.plugins", "");
    conf.setInt("zookeeper.session.timeout", 300000); // increasing session timeout for unit tests
    Configuration c = new Configuration(conf);
    System.err.println("Instantiating HBase cluster in 1 sec...");
    Thread.sleep(1000);
    hbaseCluster = new MiniHBaseCluster(c, 1, 1);
    System.err.println("Just waiting around for a bit now");
    Thread.sleep(1000);
  }

  @AfterClass
  public void stopHBase() throws Exception {

    // Stop HBase

    if (hbaseCluster != null) {
      System.err.println("\n\n\nShutting down HBase in 1 sec...\n\n\n");
      Thread.sleep(1000);
      hbaseCluster.shutdown();
      System.err.println("\n\n\nDone with HBase shutdown\n\n\n");
      hbaseCluster = null;
    }

    // Stop DFS

    if (dfsCluster != null) {
      System.err.println("\n\n\nShutting down DFS in 1 sec...\n\n\n");
      Thread.sleep(1000);
      dfsCluster.shutdown();
      System.err.println("\n\n\nDone with DFS shutdown\n\n\n");
      dfsCluster = null;
    }

    // Stop ZK

    if (zkCluster != null) {
      System.err.println("\n\n\nShutting down ZK in 1 sec...\n\n\n");
      Thread.sleep(1000);
      zkCluster.shutdown();
      System.err.println("\n\n\nDone with zk shutdown\n\n\n");
      zkCluster = null;
    }

    for (File dir : tmpDirList) {
      FileUtils.deleteDirectory(dir);
    }
  }

  // Startup/shutdown helpers


  public Path createHBaseRootDir(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path hbaseRootdir = new Path(
        fs.makeQualified(fs.getHomeDirectory()), "hbase");
    conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }

  // HRegion-level testing

  public abstract HRegion createHRegion(byte[] tableName, byte[] startKey,
                               byte[] stopKey, String callingMethod, Configuration conf,
                               byte[]... families)
    throws IOException;

  /**
   * Force and block on a flush to occur on all regions of table {@code tableName}.
   * @param tableName The table whose regions should be flushed.
   */
  public abstract void forceRegionFlush(byte[] tableName) throws IOException;

  /**
   * Force and block on a compaction on all regions of table {@code tableName}.
   * @param tableName The table whose regions should be compacted.
   * @param majorCompact Whether a major compaction should be requested.
   */
  public abstract void forceRegionCompact(byte[] tableName, boolean majorCompact) throws IOException;


  /**
   * Applies a {@link Function} on each HRegion for a given table, and returns a map of the results, keyed
   * by region name.
   * @param tableName The table whose regions should be processed.
   * @param function The function to apply on each region.
   * @param <T> The return type for the function.
   * @return
   */
  public abstract <T> Map<byte[], T> forEachRegion(byte[] tableName, Function<HRegion, T> function);

  public MiniHBaseCluster getHBaseCluster() {
    return hbaseCluster;
  }

  public static void main(String[] args) throws Exception {
    HBaseTestBase tester = new HBaseTestFactory().get();
    tester.startHBase();
  }
}
