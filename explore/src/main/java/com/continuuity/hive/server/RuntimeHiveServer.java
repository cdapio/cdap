package com.continuuity.hive.server;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.context.ContextManager;
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
 * Implementation of HiveServer internally running an instance of HiveServer2.
 */
public class RuntimeHiveServer extends HiveServer {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeHiveServer.class);

  private HiveServer2 hiveServer2;
  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final HiveConf hiveConf;
  private final int hiveServerPort;

  @Inject
  public RuntimeHiveServer(DiscoveryService discoveryService, TransactionSystemClient txClient,
                           DatasetManager datasetManager, HiveConf hiveConf,
                           @Named(Constants.Hive.SERVER_ADDRESS) InetAddress hostname,
                           @Named(Constants.Hive.SERVER_PORT) int hiveServerPort) {
    ContextManager.initialize(txClient, datasetManager);
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
