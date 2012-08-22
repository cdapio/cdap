package com.continuuity.performance.opex;

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
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class LocalHBaseOpexProvider extends OpexProvider {

  @Override
  OperationExecutor create() throws BenchmarkException {
    try {
      startHBase();
    } catch (Exception e) {
      throw new BenchmarkException(
          "Unable to start HBase: " + e.getMessage(), e);
    }
    DataFabricDistributedModule module =
        new DataFabricDistributedModule(conf);
    Injector injector = Guice.createInjector(module);
    return injector.getInstance(Key.get(OperationExecutor.class,
        Names.named("DataFabricOperationExecutor")));
  }

  @Override
  void shutdown(OperationExecutor opex) throws BenchmarkException {
    try {
      stopHBase();
    } catch (Exception e) {
      throw new BenchmarkException(
          "Unable to stop HBase: " + e.getMessage(), e);
    }
  }

  private Configuration conf;
  private MiniZooKeeperCluster zkCluster;
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
    conf = new Configuration();
    conf.setInt("hbase.regionserver.info.port", -1);
    conf.setInt("hbase.master.info.port", -1);

    // Start ZooKeeper
    System.out.println("Starting ZooKeeper ...");
    zkCluster = new MiniZooKeeperCluster(conf);
    int zkPort = zkCluster.startup(getRandomTempDir(), 1);

    // Add ZK info to conf
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(zkPort));

    // Start DFS
    System.err.println("Starting Mini DFS ...");
    File dfsPath = getRandomTempDir();
    System.setProperty("test.build.data", dfsPath.toString());
    Thread.sleep(1000);
    dfsCluster = new MiniDFSCluster.Builder(conf)
        .nameNodePort(0)
        .numDataNodes(1)
        .format(true)
        .manageDataDfsDirs(true)
        .manageNameDfsDirs(true)
        .build();
    dfsCluster.waitClusterUp();

    // Add HDFS info to conf
    conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString());

    // Start HBase
    System.out.println("Starting HBase ...");
    createHBaseRootDir(conf);
    conf.setInt("hbase.master.wait.on.regionservers.mintostart", 1);
    conf.setInt("hbase.master.wait.on.regionservers.maxtostart", 1);
    Configuration c = new Configuration(conf);
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
    zkCluster.shutdown();
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

  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--opex";
    args1[args.length + 1] = LocalHBaseOpexProvider.class.getName();
    BenchmarkRunner.main(args1);
  }
}
