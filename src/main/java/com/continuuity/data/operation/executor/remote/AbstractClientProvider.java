package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.discovery.ServicePayload;
import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractClientProvider implements OpexClientProvider {

  private static final Logger Log =
      LoggerFactory.getLogger(AbstractClientProvider.class);

  // the configuration
  CConfiguration configuration;

  // the client to discover where opex service is running
  ServiceDiscoveryClient discoveryClient;

  // the strategy we will use to choose from multiple discovered instances
  ProviderStrategy<ServicePayload> strategy;

  protected AbstractClientProvider(CConfiguration configuration) {
    this.configuration = configuration;
  }

  public void initialize() throws TException {
    // initialize the service discovery client
    this.initDiscovery();
  }

  /**
   * Initialize the service discovery client, we will reuse that
   * every time we need to create a new client
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
    try {
      this.discoveryClient = new ServiceDiscoveryClient(zookeeper);
      Log.info("Connected to service discovery. ");
    } catch (ServiceDiscoveryClientException e) {
      Log.error("Unable to start service discovery client: " + e.getMessage());
      throw new TException("Unable to start service discovery client.", e);
    }
    this.strategy = new RandomStrategy<ServicePayload>();
  }

  protected OperationExecutorClient newClient() throws TException {
    return newClient(-1);
  }

  protected OperationExecutorClient newClient(int timeout) throws TException {
    String address;
    int port;

    if (this.discoveryClient == null) {
      // if there is no discovery service, try to read host and port directly
      // from the configuration
      Log.info("Reading address and port from configuration.");
      address = configuration.get(Constants.CFG_DATA_OPEX_SERVER_ADDRESS,
          Constants.DEFAULT_DATA_OPEX_SERVER_ADDRESS);
      port = configuration.getInt(Constants.CFG_DATA_OPEX_SERVER_PORT,
          Constants.DEFAULT_DATA_OPEX_SERVER_PORT);
      Log.info("Service assumed at " + address + ":" + port);
    } else {
      try {
        // try to discover the service and pick one of the instances found
        ServiceDiscoveryClient.ServiceProvider provider =
            this.discoveryClient.getServiceProvider(
                Constants.OPERATION_EXECUTOR_SERVICE_NAME);
        ServiceInstance<ServicePayload>
            instance = strategy.getInstance(provider);
        // found an instance, get its host name and port
        address = instance.getAddress();
        port = instance.getPort();
      } catch (Exception e) {
        Log.error("Unable to discover opex service: " + e.getMessage());
        throw new TException("Unable to discover opex service.", e);
      }
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
