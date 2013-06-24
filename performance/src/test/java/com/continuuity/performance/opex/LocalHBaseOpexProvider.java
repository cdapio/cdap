package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * OpexProvider class that can be used to run benchmarks against Mini-HBase.
 */
public class LocalHBaseOpexProvider extends OpexProvider {

  private static final Logger LOG = LoggerFactory.getLogger(LocalHBaseOpexProvider.class);

  /** Override this to enable native queues. */
  protected boolean useNativeQueues() {
    return false;
  }

  @Override
  OperationExecutor create() throws BenchmarkException {
    try {
      startHBase();
    } catch (Exception e) {
      throw new BenchmarkException(
          "Unable to start HBase: " + e.getMessage(), e);
    }
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(
        DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES,
        this.useNativeQueues());
    DataFabricDistributedModule module =
        new DataFabricDistributedModule(hbaseConf, conf);
    Injector injector = Guice.createInjector(module);
    return injector.getInstance(Key.get(OperationExecutor.class,
        Names.named("DataFabricOperationExecutor")));
  }

  @Override
  void shutdown(OperationExecutor opex) {
    try {
      stopHBase();
    } catch (Exception e) {
      LOG.error("Unable to stop HBase: {}", e.getMessage(), e);
    }
  }

  private Configuration hbaseConf;
  private InMemoryZookeeper zkCluster;
  private MiniDFSCluster dfsCluster;
  private MiniHBaseCluster hbaseCluster;

  // Temporary directories
  private final Random r = new Random();

  private File getRandomTempDir() {
    File file = new File(System.getProperty("java.io.tmpdir"),
        Integer.toString(Math.abs(r.nextInt())));
    if (!file.mkdir()) {
      throw new RuntimeException("Unable to create temp directory");
    }
    return file;
  }

  private void startHBase() throws Exception {

    // disable UIs to prevent port conflicts)
    hbaseConf = new Configuration();
    hbaseConf.setInt("hbase.regionserver.info.port", -1);
    hbaseConf.setInt("hbase.master.info.port", -1);

    // Start ZooKeeper
    System.out.println("Starting ZooKeeper ...");
    zkCluster = new InMemoryZookeeper();
    int zkPort = zkCluster.getPort();

    // Add ZK info to hbaseConf
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(zkPort));

    // Start DFS
    System.err.println("Starting Mini DFS ...");
    File dfsPath = getRandomTempDir();
    System.setProperty("test.build.data", dfsPath.toString());
    Thread.sleep(1000);
    dfsCluster = new MiniDFSCluster.Builder(hbaseConf)
        .nameNodePort(0)
        .numDataNodes(1)
        .format(true)
        .manageDataDfsDirs(true)
        .manageNameDfsDirs(true)
        .build();
    dfsCluster.waitClusterUp();

    // Add HDFS info to hbaseConf
    hbaseConf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString());

    // Start HBase
    System.out.println("Starting HBase ...");
    createHBaseRootDir(hbaseConf);
    hbaseConf.setInt("hbase.master.wait.on.regionservers.mintostart", 1);
    hbaseConf.setInt("hbase.master.wait.on.regionservers.maxtostart", 1);
    Configuration c = new Configuration(hbaseConf);
    hbaseCluster = new MiniHBaseCluster(c, 1, 1);

    // Sleep a short time
    Thread.sleep(1000);
    System.out.println("Ready.");
  }

  private void stopHBase() throws Exception {

    // Stop HBase
    System.out.println("Shutting down HBase ...");
    hbaseCluster.shutdown();
    hbaseCluster = null;

    // Stop DFS
    System.out.println("Shutting down DFS ...");
    dfsCluster.shutdown();
    dfsCluster = null;

    // Stop ZK
    System.out.println("Shutting down ZooKeeper ...");
    zkCluster.stop();
    zkCluster = null;

    System.out.println("Done.");
  }

  // Startup/shutdown helpers
  private Path createHBaseRootDir(Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path hbaseRootdir = new Path(
        fs.makeQualified(fs.getHomeDirectory()), "hbase");
    conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }

  public static void main(String[] args) throws Exception {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--opex";
    args1[args.length + 1] = LocalHBaseOpexProvider.class.getName();
    BenchmarkRunner.main(args1);
  }
}
