package com.continuuity.gateway.util;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.discovery.ServicePayload;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.remote.Constants;
import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceDiscovery {

  private static final Logger Log =
      LoggerFactory.getLogger(ServiceDiscovery.class);

  // the configuration
  CConfiguration configuration;

  // the client to discover where opex service is running
  ServiceDiscoveryClient discoveryClient;

  // the strategy we will use to choose from multiple discovered instances
  ProviderStrategy<ServicePayload> strategy;

  public ServiceDiscovery(CConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Initialize the service discovery client, we will reuse that
   * every time we need to create a new client
   * @throws java.io.IOException
   */
  public void initialize() throws ServerException {

    // try to find the zookeeper ensemble in the config
    String zookeeper = configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    if (zookeeper == null) {
      // no zookeeper, look for the port and use localhost
      String message = "Zookeeper Ensemble not configured. Unable to " +
          "initialize service discovery";
      Log.error(message);
      throw new ServerException(message);
    }
    // attempt to discover the service
    try {
      this.discoveryClient = new ServiceDiscoveryClient(zookeeper);
      Log.trace("Connected to service discovery. ");
    } catch (ServiceDiscoveryClientException e) {
      Log.error("Unable to start service discovery client: " + e.getMessage());
      throw new ServerException(
          "Unable to start service discovery client.", e);
    }
    this.strategy = new RandomStrategy<ServicePayload>();
  }

  public ImmutablePair<String, Integer> getServiceAddress(String serviceName)
      throws ServerException {
    String address;
    int port;
    try {
      // try to discover the service and pick one of the instances found
      ServiceDiscoveryClient.ServiceProvider provider =
          this.discoveryClient.getServiceProvider(serviceName);
      ServiceInstance<ServicePayload>
          instance = strategy.getInstance(provider);
      if (instance != null) {
        // found an instance, get its host name and port
        address = instance.getAddress();
        port = instance.getPort();
        Log.trace("Service discovered at " + address + ":" + port);
        return new ImmutablePair<String, Integer>(address, port);
      }
    } catch (Exception e) {
      String message = String.format("Error while discovering service '%s'. " +
          "Reason: %s", serviceName, e.getMessage());
      Log.error(message);
      throw new ServerException(message, e);
    }
    // no instance found
    return null;
  }

}

