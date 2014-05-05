package com.continuuity.hive;

import com.continuuity.common.utils.PortDetector;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hive.service.server.HiveServer2;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Starts up an embedded hive server for testing.
 */
public class EmbeddedHiveServer extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedHiveServer.class);

  private HiveServer2 hiveServer2;
  private int hiveServerPort;

  private MiniMRCluster miniMR;
  private MiniDFSCluster miniDFS;

  public int getHiveServerPort() {
    return hiveServerPort;
  }

  @Override
  protected void before() throws Throwable {
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
    writeHiveConf();

    // Start Hive
    HiveConf hiveConf = new HiveConf();
    LOG.error("Starting hive server...");
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    TimeUnit.SECONDS.sleep(1);
  }

  @Override
  protected void after() {
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

  private void writeHiveConf() throws Exception {
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

    File newHiveConf = new File(confDir, "hive-site.xml");
    hiveConf.writeXml(new FileOutputStream(newHiveConf));
    LOG.info("Wrote hive conf into {}", newHiveConf.getAbsolutePath());
  }
}
