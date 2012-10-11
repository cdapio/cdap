package com.continuuity.gateway.accessor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.discovery.ServicePayload;
import com.continuuity.data.operation.executor.remote.Constants;
import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryProviderDiscovery {

  private static final Logger Log =
      LoggerFactory.getLogger(QueryProviderDiscovery.class);

  // the configuration
  CConfiguration configuration;

  // the client to discover where opex service is running
  ServiceDiscoveryClient discoveryClient;

  // the strategy we will use to choose from multiple discovered instances
  ProviderStrategy<ServicePayload> strategy;

  public QueryProviderDiscovery(CConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Initialize the service discovery client, we will reuse that
   * every time we need to create a new client
   * @throws java.io.IOException
   */
  public void initialize() throws Exception {

    // try to find the zookeeper ensemble in the config
    String zookeeper = configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    if (zookeeper == null) {
      // no zookeeper, look for the port and use localhost
      String message = "Zookeeper Ensemble not configured. Unable to " +
          "initialize service discovery";
      Log.error(message);
      throw new Exception(message);
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

  protected String getServiceAddress(String serviceName) throws Exception {
    String address;
    int port;
    try {
      String lookupName = "query." + serviceName;
      // try to discover the service and pick one of the instances found
      ServiceDiscoveryClient.ServiceProvider provider =
          this.discoveryClient.getServiceProvider( lookupName);
      ServiceInstance<ServicePayload>
          instance = strategy.getInstance(provider);
      // found an instance, get its host name and port
      if (instance != null) {
        address = instance.getAddress();
        port = instance.getPort();
        Log.info("Service discovered at " + address + ":" + port);
        return address + ":" + port;
      } else {
        return null;
      }
    } catch (Exception e) {
      Log.error("Unable to discover query service '" + serviceName + "': "
          + e.getMessage());
      throw new Exception("Unable to discover opex service.", e);
    }
  }
}

