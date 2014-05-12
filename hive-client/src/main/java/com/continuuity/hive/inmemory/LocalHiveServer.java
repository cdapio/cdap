package com.continuuity.hive.inmemory;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.hive.HiveServer;
import com.continuuity.test.internal.TempFolder;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.io.File;
import java.io.FileOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hive.service.server.HiveServer2;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LocalHiveServer extends AbstractIdleService implements HiveServer {

  private static final Logger LOG = LoggerFactory.getLogger(LocalHiveServer.class);

  private HiveServer2 hiveServer2;
  private int hiveServerPort;
  private int hiveMetaStorePort;

  private final DiscoveryService discoveryService;
  private static DiscoveryServiceClient discoveryServiceClient;
  private static InMemoryTransactionManager inMemoryTransactionManager;
  private final InetAddress hostname;

  @Inject
  public LocalHiveServer(DiscoveryService discoveryService, InMemoryTransactionManager inMemoryTransactionManager,
                         DiscoveryServiceClient discoveryServiceClient,
                         @Named(Constants.Hive.SERVER_ADDRESS) InetAddress hostname) {
    this.discoveryService = discoveryService;
    LocalHiveServer.discoveryServiceClient = discoveryServiceClient;
    LocalHiveServer.inMemoryTransactionManager = inMemoryTransactionManager;
    this.hostname = hostname;
  }

  public static DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  public static InMemoryTransactionManager getInMemoryTransactionManager() {
    return inMemoryTransactionManager;
  }

  private HiveConf createHiveConf() throws Exception {
    URL url = this.getClass().getClassLoader().getResource("hive-site-placeholder.xml");
    assert url != null;
    File confDir = new File(url.toURI()).getParentFile();

    HiveConf hiveConf = new HiveConf();
    hiveConf.clear();
    hiveConf.set("hive.server2.authentication", "NOSASL");
    hiveConf.set("hive.metastore.sasl.enabled", "false");
    hiveConf.set("hive.server2.enable.doAs", "false");
    hiveConf.set("hive.exec.mode.local.auto", "true");
    hiveConf.set("hive.exec.submitviachild", "false");
    hiveConf.set("mapreduce.framework.name", "local");
    // TODO: get local data dir from CConf
    hiveConf.set("hive.metastore.warehouse.dir", "/tmp/hive-warehouse");

    hiveServerPort = PortDetector.findFreePort();
    hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

    // Register with discovery service.
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

    hiveMetaStorePort = PortDetector.findFreePort();
    hiveConf.set("hive.metastore.uris", "thrift://" + finalSocketAddress.getHostName() + ":" + hiveMetaStorePort);

    File newHiveConf = new File(confDir, "hive-site.xml");
    hiveConf.writeXml(new FileOutputStream(newHiveConf));
    LOG.info("Wrote hive conf into {}", newHiveConf.getAbsolutePath());

    return hiveConf;
  }

  private File writeConf(HiveConf hiveConf) throws Exception {
    TempFolder tempFolder = new TempFolder();

    File hiveConfFile = (tempFolder.newFile("temp-hive-site.xml"));
    hiveConf.writeXml(new FileOutputStream(hiveConfFile));
    LOG.info("Wrote hive conf into {}", hiveConfFile.getAbsolutePath());
    return hiveConfFile;
  }

  @Override
  protected void startUp() throws Exception {
    HiveConf hiveConf = createHiveConf();
    final File hiveConfFile = writeConf(hiveConf);

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
  }

  private void waitForPort(int port) throws Exception {
    final int maxTries = 20;
    int tries = 0;

    Socket socket = null;
    try {
      do {
        TimeUnit.MILLISECONDS.sleep(500);
        try {
          // TODO: change it to hive.server.bind.address
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
}
