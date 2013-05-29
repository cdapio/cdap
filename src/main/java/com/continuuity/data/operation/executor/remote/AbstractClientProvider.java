package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.ZKDiscoveryService;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import org.apache.commons.lang.time.StopWatch;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * An abstract opex client provider that implements common functionality.
 */
public abstract class AbstractClientProvider implements OpexClientProvider {

  private static final long DISCOVERY_TIMEOUT_SEC = 10;

  private static final Logger Log =
      LoggerFactory.getLogger(AbstractClientProvider.class);

  // the configuration
  CConfiguration configuration;

  // the endpoint strategy for service discovery.
  EndpointStrategy endpointStrategy;

  protected AbstractClientProvider(CConfiguration configuration) {
    this.configuration = configuration;
  }

  public void initialize() throws TException {
    // initialize the service discovery client
    this.initDiscovery();
  }

  /**
   * Initialize the service discovery client, we will reuse that
   * every time we need to create a new client.
   * @throws IOException
   */
  public void initDiscovery() throws TException {
    // try to find the zookeeper ensemble in the config
    String zookeeper = configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    if (zookeeper == null) {
      // no zookeeper, look for the port and use localhost
      Log.info("Zookeeper Ensemble not configured. Skipping service discovery");
      return;
    }
    // attempt to discover the service
    // Ideally the DiscoveryServiceClient should be injected so that we don't need to create a new ZK client
    // Also, there should be a stop() method for lifecycle management to stop the ZK client
    // Although it's ok for now as ZKClientService uses daemon thread only
    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zookeeper).build(),
            RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
          )
        ));
    zkClientService.startAndWait();
    endpointStrategy = new RandomEndpointStrategy(
      new ZKDiscoveryService(zkClientService).discover(Constants.OPERATION_EXECUTOR_SERVICE_NAME));
  }

  protected OperationExecutorClient newClient() throws TException {
    return newClient(-1);
  }

  protected OperationExecutorClient newClient(int timeout) throws TException {
    String address;
    int port;

    if (endpointStrategy == null) {
      // if there is no discovery service, try to read host and port directly
      // from the configuration
      Log.info("Reading address and port from configuration.");
      address = configuration.get(Constants.CFG_DATA_OPEX_SERVER_ADDRESS,
                                  Constants.DEFAULT_DATA_OPEX_SERVER_ADDRESS);
      port = configuration.getInt(Constants.CFG_DATA_OPEX_SERVER_PORT,
                                  Constants.DEFAULT_DATA_OPEX_SERVER_PORT);
      Log.info("Service assumed at " + address + ":" + port);
    } else {
      Discoverable endpoint = endpointStrategy.pick();
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      stopWatch.split();
      while (endpoint == null && (stopWatch.getSplitTime() / 1000) < DISCOVERY_TIMEOUT_SEC) {
        try {
          TimeUnit.MILLISECONDS.sleep(500);
          endpoint = endpointStrategy.pick();
          stopWatch.split();
        } catch (InterruptedException e) {
          break;
        }
      }
      if (endpoint == null) {
        Log.error("Unable to discover opex service.");
        throw new TException("Unable to discover opex service.");
      }
      address = endpoint.getSocketAddress().getHostName();
      port = endpoint.getSocketAddress().getPort();
      Log.info("Service discovered at " + address + ":" + port);
    }

    // now we have an address and port, try to connect a client
    if (timeout < 0) {
      timeout = configuration.getInt(Constants.CFG_DATA_OPEX_CLIENT_TIMEOUT,
          Constants.DEFAULT_DATA_OPEX_CLIENT_TIMEOUT);
    }
    Log.info("Attempting to connect to Operation Executor service at " +
        address + ":" + port + " with timeout " + timeout + " ms.");
    // thrift transport layer
    TTransport transport =
        new TFramedTransport(new TSocket(address, port, timeout));
    try {
      transport.open();
    } catch (TTransportException e) {
      Log.error("Unable to connect to opex service: " + e.getMessage());
      throw e;
    }
    // and create a thrift client
    OperationExecutorClient newClient = new OperationExecutorClient(transport);

    Log.info("Connected to Operation Executor service at " +
        address + ":" + port);
    return newClient;
  }
}
