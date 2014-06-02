package com.continuuity.hive.server;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.context.ContextManager;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
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

  private IsolatedHiveServer hiveServer2;
  private ClassLoader hiveClassLoader;
  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private Cancellable discoveryCancel;

  @Inject
  public RuntimeHiveServer(@Named(Constants.Explore.HIVE_CLASSSLOADER) ClassLoader hiveClassLoader,
                           DiscoveryService discoveryService, TransactionSystemClient txClient,
                           DatasetFramework datasetFramework,
                           @Named(Constants.Hive.SERVER_ADDRESS) InetAddress hostname) {
    this.hiveClassLoader = hiveClassLoader;
    ContextManager.initialize(txClient, datasetFramework);
    this.discoveryService = discoveryService;
    this.hostname = hostname;
  }

  @Override
  protected void startUp() throws Exception {
    // Start Hive Server2
    int hiveServerPort = PortDetector.findFreePort();
    System.setProperty("hive.server2.thrift.port", String.valueOf(hiveServerPort));
    System.setProperty("hive.server2.thrift.bind.host", hostname.getCanonicalHostName());

    LOG.info("Starting hive server on port {}...", hiveServerPort);
    hiveServer2 = new IsolatedHiveServer(hiveClassLoader);
    hiveServer2.start();
    waitForPort(hostname.getCanonicalHostName(), hiveServerPort);

    // Register HiveServer2 with discovery service.
    InetSocketAddress socketAddress = new InetSocketAddress(hostname, hiveServerPort);
    InetAddress address = socketAddress.getAddress();
    if (address.isAnyLocalAddress()) {
      address = InetAddress.getLocalHost();
    }
    final InetSocketAddress finalSocketAddress = new InetSocketAddress(address, hiveServerPort);

    discoveryCancel = discoveryService.register(new Discoverable() {
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
    if (discoveryCancel != null) {
      discoveryCancel.cancel();
    }
  }

  /**
   * Util method that waits for a 3rd party service, running on a given port, to be started.
   * @param host host the service is running on
   * @param port port the service is running on
   */
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
