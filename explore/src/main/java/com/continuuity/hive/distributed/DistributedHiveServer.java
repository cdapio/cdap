package com.continuuity.hive.distributed;

import com.continuuity.common.conf.Constants;
import com.continuuity.hive.HiveServer;

import com.google.common.base.Preconditions;
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
 *
 */
public class DistributedHiveServer extends AbstractIdleService implements HiveServer {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedHiveServer.class);

  private HiveServer2 hiveServer2;

  private final DiscoveryService discoveryService;
  private final HiveConf hiveConf;
  private final InetAddress hostname;

  @Inject
  public DistributedHiveServer(DiscoveryService discoveryService, HiveConf hiveConf,
                               @Named(Constants.Hive.Container.SERVER_ADDRESS) InetAddress hostname) {
    this.discoveryService = discoveryService;
    this.hiveConf = hiveConf;
    this.hostname = hostname;
  }

  @Override
  protected void startUp() throws Exception {
    int hiveServerPort = hiveConf.getInt("hive.server2.thrift.port", 0);
    Preconditions.checkArgument(hiveServerPort != 0, "Hive server port must have been set.");

    // Start Hive Server2
    LOG.info("Starting hive server on {}:{}, code version {}...", hostname, hiveServerPort, 2.1);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    waitForPort(hostname.getHostName(), hiveServerPort);
    LOG.info("Hive server started...");

    // Register hive server with discovery service.
    final InetSocketAddress socketAddress = new InetSocketAddress(hostname, hiveServerPort);
    // todo are those lines necessary? - saw that in appfabric service
//    InetAddress address = socketAddress.getAddress();
//    if (address.isAnyLocalAddress()) {
//      address = InetAddress.getLocalHost();
//    }
//    final InetSocketAddress finalSocketAddress = new InetSocketAddress(address, hiveServerPort);

    discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.HIVE;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return socketAddress;
      }
    });

    LOG.info("Hive server registered with zookeeper...");
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
