package com.continuuity.hive.inmemory;

import com.continuuity.common.utils.PortDetector;
import com.continuuity.hive.HiveServer;
import com.google.common.util.concurrent.AbstractIdleService;
import java.io.File;
import java.io.FileOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LocalHiveServer extends AbstractIdleService implements HiveServer {

  // TODO now implement service discovery

  private static final Logger LOG = LoggerFactory.getLogger(LocalHiveServer.class);

  private HiveServer2 hiveServer2;
  private int hiveServerPort;
  private int hiveMetaStorePort;

  private MiniMRCluster miniMR;
  private MiniDFSCluster miniDFS;

  private void writeYarnConf() throws Exception {
    URL url = this.getClass().getClassLoader().getResource("hive-site-placeholder.xml");
    assert url != null;
    File confDir = new File(url.toURI()).getParentFile();

    Configuration conf;

    if (miniMR == null) {
      conf = new Configuration();
      conf.clear();
    } else {
      conf = miniMR.createJobConf();
    }

    conf.set("yarn.application.classpath", System.getProperty("java.class.path").replaceAll(":", ","));

    File newYarnConf = new File(confDir, "yarn-site.xml");
    conf.writeXml(new FileOutputStream(newYarnConf, false));
    LOG.info("Wrote yarn conf into {}", newYarnConf.getAbsolutePath());
  }

  private File writeHiveConf() throws Exception {
    URL url = this.getClass().getClassLoader().getResource("hive-site-placeholder.xml");
    assert url != null;
    File confDir = new File(url.toURI()).getParentFile();

    HiveConf hiveConf = new HiveConf(miniMR.createJobConf(), HiveConf.class);
    hiveConf.clear();
    hiveConf.set("hive.server2.authentication", "NOSASL");
    hiveConf.set("hive.metastore.sasl.enabled", "false");
    hiveConf.set("hive.server2.enable.doAs", "false");
    hiveConf.set("hive.exec.mode.local.auto", "false");
    hiveConf.set("mapreduce.framework.name", "yarn");

    hiveServerPort = PortDetector.findFreePort();
    hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

    hiveMetaStorePort = PortDetector.findFreePort();
    hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hiveMetaStorePort);

    File newHiveConf = new File(confDir, "hive-site.xml");
    hiveConf.writeXml(new FileOutputStream(newHiveConf));
    LOG.info("Wrote hive conf into {}", newHiveConf.getAbsolutePath());

    return newHiveConf;
  }

  @Override
  protected void startUp() throws Exception {
    String javaHome = System.getenv("JAVA_HOME");
    Assert.assertNotNull("JAVA_HOME is not set", javaHome);
    Assert.assertFalse("JAVA_HOME is not set", javaHome.isEmpty());

    writeYarnConf();

    Configuration conf = new Configuration();

    // Start HDFS
    miniDFS = new MiniDFSCluster.Builder(conf).build();

    // Start Yarn
    System.setProperty("hadoop.log.dir", "/tmp");
    int numTaskTrackers = 1;
    int numTaskTrackerDirectories = 1;
    String[] racks = null;
    String[] hosts = null;
    JobConf jobConf = new JobConf(conf);
    miniMR = new MiniMRCluster(numTaskTrackers, miniDFS.getFileSystem().getUri().toString(),
        numTaskTrackerDirectories, racks, hosts, jobConf);

    writeYarnConf();
    final File hiveConfFile = writeHiveConf();

    // Start Hive MetaStore
    LOG.info("Starting hive metastore on port {}...", hiveMetaStorePort);
    Thread metaStoreRunner = new Thread(
        new Runnable() {
          @Override
          public void run() {
            try {
              HiveMetaStore.main(new String[]{"-v", "-p", Integer.toString(hiveMetaStorePort),
                  "--hiveconf", hiveConfFile.getAbsolutePath()});
            } catch (Throwable throwable) {
              LOG.error("Exception while starting Hive MetaStore: ", throwable);
            }
          }
        });
    metaStoreRunner.setDaemon(true);
    metaStoreRunner.start();
    waitForPort(hiveMetaStorePort);

    // Start Hive Server2
    HiveConf hiveConf = new HiveConf();
    LOG.error("Starting hive server on port {}...", hiveServerPort);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    waitForPort(hiveServerPort);
  }

  @Override
  protected void shutDown() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }

    if (miniMR != null) {
      miniMR.shutdown();
    }

    if (miniDFS != null) {
      miniDFS.shutdown();
    }
  }

  private void waitForPort(int port) throws Exception {
    final int maxTries = 20;
    int tries = 0;

    Socket socket = null;
    try {
      do {
        TimeUnit.MILLISECONDS.sleep(500);
        try {
          socket = new Socket("localhost", port);
        } catch (ConnectException e) {
          if (++tries > maxTries) {
            throw e;
          }
        }
      } while (socket == null);
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  @Override
  public int getHiveMetaStorePort() {
    return hiveMetaStorePort;
  }

  @Override
  public int getHiveServerPort() {
    return hiveServerPort;
  }
}
