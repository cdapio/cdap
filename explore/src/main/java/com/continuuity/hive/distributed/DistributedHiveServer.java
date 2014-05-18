package com.continuuity.hive.distributed;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.inmemory.LocalHiveServer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DistributedHiveServer extends AbstractIdleService implements HiveServer {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedHiveServer.class);

  private static File hiveConfFile = createHiveConf();
  private static int hiveServerPort;

  private HiveServer2 hiveServer2;

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;

  @Inject
  public DistributedHiveServer(DiscoveryService discoveryService,
                              @Named(Constants.Hive.SERVER_ADDRESS) InetAddress hostname) {
    this.discoveryService = discoveryService;
    this.hostname = hostname;
  }

  private static File createHiveConf() {
    try {
      URL url = LocalHiveServer.class.getClassLoader().getResource("hive-site-placeholder.xml");
      assert url != null;
      File confDir = new File(url.toURI()).getParentFile();

      Configuration configuration = new Configuration();
      configuration.clear();
      configuration.set("hive.server2.authentication", "NOSASL");
      configuration.set("hive.metastore.sasl.enabled", "false");
      configuration.set("hive.server2.enable.doAs", "false");
//      configuration.set("hive.exec.mode.local.auto", "true");
//      configuration.set("hive.exec.submitviachild", "false");
      configuration.set("mapreduce.framework.name", "yarn");
      // TODO: get local data dir from CConf
//      configuration.set("hive.metastore.warehouse.dir", "/tmp/hive-warehouse");

      hiveServerPort = PortDetector.findFreePort();
      configuration.setInt("hive.server2.thrift.port", hiveServerPort);

      // todo temporary hard coded address
      configuration.set("hive.metastore.uris", "thrift://hive-distributed728-1001.dev.continuuity.net:9083");

      File newHiveConf = new File(confDir, "hive-site.xml");
      configuration.writeXml(new FileOutputStream(newHiveConf));
      newHiveConf.deleteOnExit();
      LOG.info("Wrote hive conf into {}", newHiveConf.getAbsolutePath());

      return newHiveConf;
    } catch (Exception e) {
      LOG.error("Got exception while trying to create hive-site.xml", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    HiveConf hiveConf = new HiveConf();

    // Start Hive Server2
    LOG.error("Starting hive server on port {}...", hiveServerPort);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    waitForPort(hostname.getHostName(), hiveServerPort);

    // Register hive server with discovery service.
    InetSocketAddress socketAddress = new InetSocketAddress(hostname, hiveServerPort);
    InetAddress address = socketAddress.getAddress();
    if (address.isAnyLocalAddress()) {
      address = InetAddress.getLocalHost();
    }
    final InetSocketAddress finalSocketAddress = new InetSocketAddress(address, hiveServerPort);

    discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.HIVE;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return finalSocketAddress;
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  private void waitForPort(String hostname, int port) throws Exception {
    final int maxTries = 20;
    int tries = 0;

    Socket socket = null;
    try {
      do {
        TimeUnit.MILLISECONDS.sleep(500);
        try {
          socket = new Socket(hostname, port);
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
}
