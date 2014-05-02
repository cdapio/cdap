package com.continuuity.hive;

import com.continuuity.common.utils.PortDetector;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Starts up an embedded hive server for testing.
 */
public class EmbeddedHiveServer extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedHiveServer.class);

  private HiveServer2 hiveServer2;
  private int serverPort;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  public int getServerPort() {
    return serverPort;
  }

  @Override
  protected void before() throws Throwable {
    // TODO: use dynamic tmp dir for warehouse dir
    // Currently not possible to dynamically set hive.metastore.warehouse.dir since the HiveConf object does not
    // passed on to Hive Meta Store. Instead Hive Meta Store instantiates its own HiveConf.
    HiveConf hiveConf = new HiveConf();
//    hiveConf.set("hive.metastore.warehouse.dir",
//                 new File(temporaryFolder.newFolder(), "/user/hive/warehouse").getAbsolutePath());

    LOG.info("hive.metastore.warehouse.dir=" + hiveConf.get("hive.metastore.warehouse.dir"));

    serverPort = PortDetector.findFreePort();
    hiveConf.setInt("hive.server2.thrift.port", serverPort);

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
  }
}
