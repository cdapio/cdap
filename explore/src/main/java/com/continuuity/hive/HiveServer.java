package com.continuuity.hive;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * Hive Server 2 service.
 */
public class HiveServer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(HiveServer.class);

  private HiveServer2 hiveServer2;

  private static TransactionSystemClient txClient;
  private static DatasetManager datasetManager;

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;

  private final HiveConf hiveConf;
  private final int hiveServerPort;

  @Inject
  public HiveServer(DiscoveryService discoveryService, TransactionSystemClient txClient,
                    DatasetManager datasetManager, HiveConf hiveConf,
                    @Named(Constants.Hive.SERVER_ADDRESS) InetAddress hostname,
                    @Named(Constants.Hive.SERVER_PORT) int hiveServerPort) {
    HiveServer.txClient = txClient;
    HiveServer.datasetManager = datasetManager;
    this.discoveryService = discoveryService;
    this.hostname = hostname;
    this.hiveConf = hiveConf;
    this.hiveServerPort = hiveServerPort;
  }

  @Override
  protected void startUp() throws Exception {
    // Start Hive Server2
    LOG.info("Starting hive server on port {}...", hiveServerPort);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    waitForPort(hostname.getHostName(), hiveServerPort);

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
  }

  public static DatasetManager getDatasetManager() {
    return datasetManager;
  }

  public static TransactionSystemClient getTransactionSystemClient() {
    return txClient;
  }

  @Override
  protected void shutDown() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  public static void waitForPort(String host, int port) throws Exception {
    final int maxTries = 20;
    int tries = 0;

    Socket socket = null;
    try {
      do {
        try {
          socket = new Socket(host, port);
        } catch (ConnectException e) {
          if (++tries > maxTries) {
            throw e;
          }
          TimeUnit.MILLISECONDS.sleep(500);
        }
      } while (socket == null);
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }
}
