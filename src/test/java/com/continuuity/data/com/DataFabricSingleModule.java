package com.continuuity.data.com;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.engine.hbase.HBaseNativeOVCTableHandle;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;
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
import java.util.Random;

public class DataFabricSingleModule extends AbstractModule {

  private static final Logger Log =
      LoggerFactory.getLogger(DataFabricSingleModule.class);

  private CConfiguration conf;

  public static final String CONF_ENABLE_NATIVE_QUEUES =
      "fabric.queue.hbase.native";

  private static final boolean CONF_ENABLE_NATIVE_QUEUES_DEFAULT = true;

  private Configuration hbaseConf;
  private InMemoryZookeeper zkCluster;
  private MiniDFSCluster dfsCluster;
  private MiniHBaseCluster hbaseCluster;

  // Temporary directories
  private final Random r = new Random();


  /**
   * Create a module with default configuration for HBase and Continuuity
   */
  public DataFabricSingleModule() {
//    this.conf = loadConfiguration();
//    this.hbaseConf = HBaseConfiguration.create(conf);
    // disable UIs to prevent port conflicts)
    try {
      create();
    } catch(Exception e) {
    }
  }

  /**
   * Create a module with custom configuration for HBase,
   * and defaults for Continuuity
   */
  public DataFabricSingleModule(Configuration conf) {
//    this.hbaseConf = new Configuration(conf);
//    this.conf = loadConfiguration();
    // disable UIs to prevent port conflicts)
    try {
      create();
    } catch(Exception e) {
    }
  }

  /**
   * Create a module with separate, custom configurations for HBase
   * and for Continuuity
   */
  public DataFabricSingleModule(Configuration conf,
                                CConfiguration cconf) {
//    this.hbaseConf = new Configuration(conf);
//    this.conf = cconf;
    // disable UIs to prevent port conflicts)
    try {
      create();
    } catch(Exception e) {
    }
  }

  /**
   * Create a module with custom configuration, which will
   * be used both for HBase and for Continuuity
   */
  public DataFabricSingleModule(CConfiguration conf) {
//    this.hbaseConf = new Configuration(conf);
//    this.conf = conf;
    // disable UIs to prevent port conflicts)
    try {
      create();
    } catch(Exception e) {
    }
  }
  /** override this to enable native queues */
  protected boolean useNativeQueues() {
    return false;
  }

  private CConfiguration loadConfiguration() {
    CConfiguration conf = CConfiguration.create();

    // this expects the port and number of threads for the opex service
    // - data.opex.server.port <int>
    // - data.opex.server.threads <int>
    // this expects the zookeeper quorum for continuuity and for hbase
    // - zookeeper.quorum host:port,...
    // - hbase.zookeeper.quorum host:port,...
    return conf;
  }

  @Override
  public void configure() {

    Class<? extends OVCTableHandle> ovcTableHandle = HBaseOVCTableHandle.class;
    // Check if native hbase queue handle should be used
    if (conf.getBoolean(CONF_ENABLE_NATIVE_QUEUES,
        CONF_ENABLE_NATIVE_QUEUES_DEFAULT)) {
      ovcTableHandle = HBaseNativeOVCTableHandle.class;
    }
    Log.info("Table Handle is " + ovcTableHandle.getName());

    // Bind our implementations
    // For now, just bind to in-memory omid oracles
    bind(TimestampOracle.class).
      to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);

    bind(TransactionOracle.class).to(MemoryOracle.class);

    // Bind remote operation executor
    bind(OperationExecutor.class).
//      to(RemoteOperationExecutor.class).in(Singleton.class);
      to(OmidTransactionalOperationExecutor.class).in(Singleton.class);

    // For data fabric, bind to Omid and HBase
    bind(OperationExecutor.class)
        .annotatedWith(Names.named("DataFabricOperationExecutor"))
        .to(OmidTransactionalOperationExecutor.class)
        .in(Singleton.class);

    bind(OVCTableHandle.class).to(ovcTableHandle);

    // Bind HBase configuration into ovctable
    bind(Configuration.class)
        .annotatedWith(Names.named("HBaseOVCTableHandleConfig"))
        .toInstance(hbaseConf);

    // Bind our configurations
//    bind(CConfiguration.class)
//        .annotatedWith(Names.named("RemoteOperationExecutorConfig"))
//        .toInstance(conf);
  }

  public CConfiguration getConfiguration() {
    return this.conf;
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
  OperationExecutor create() throws Exception {
    try {
      startHBase();
    } catch (Exception e) {
      throw new Exception("Unable to start HBase: " + e.getMessage(), e);
    }
    this.conf = CConfiguration.create();
    this.conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES,
      this.useNativeQueues());
    DataFabricDistributedModule module =
      new DataFabricDistributedModule(this.hbaseConf, this.conf);
    Injector injector = Guice.createInjector(module);
    return injector.getInstance(Key.get(OperationExecutor.class,
      Names.named("DataFabricOperationExecutor")));
  }

  void shutdown(OperationExecutor opex) throws Exception {
    try {
      stopHBase();
    } catch (Exception e) {
      throw new Exception("Unable to stop HBase: " + e.getMessage(), e);
    }
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
    Path hbaseRootdir = new Path(fs.makeQualified(fs.getHomeDirectory()), "hbase");
    conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }
  private File getRandomTempDir() {
    File file = new File(System.getProperty("java.io.tmpdir"),
      Integer.toString(Math.abs(r.nextInt())));
    if (!file.mkdir()) {
      throw new RuntimeException("Unable to create temp directory");
    }
    return file;
  }
}
