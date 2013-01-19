package com.continuuity.common.discovery;

import com.continuuity.common.zookeeper.ZookeeperClientProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.x.discovery.*;
import com.netflix.curator.x.discovery.details.InstanceProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * A Service registration client that supports registering a service
 * with a central service discovery service. This essentially is a wrapper
 * around the Netflix curator for managing states.
 */
public class ServiceDiscoveryClient implements Closeable {
  public static final String SERVICE_PATH = "/continuuity/system/services";

  private static final int NUMBER_OF_RETRY = 100;
  private static final int TIME_BETWEEN_RETRIES = 1000;

  private final String connectionString;
  private ServiceDiscovery<ServicePayload> discovery;

  private boolean started;
  private CuratorFramework client;

  /**
   * Constructs a service registration using an connection string.
   *
   * @param connectionString the connect string to ZK ensemble.
   * @throws ServiceDiscoveryClientException
   */
  public ServiceDiscoveryClient(String connectionString) throws ServiceDiscoveryClientException {
    connectionString = Preconditions.checkNotNull(connectionString);
    this.connectionString = connectionString;
    start();
  }

  /**
   * Starts the curator framework and discovery mechanism.
   * @throws ServiceDiscoveryClientException
   */
  private void start() throws ServiceDiscoveryClientException {
    try {
      // Create a curator client.
      client = ZookeeperClientProvider.getClient(
        connectionString, NUMBER_OF_RETRY, TIME_BETWEEN_RETRIES
      );

      // Create a service discovery that allocated ServiceProviders.
      discovery = ServiceDiscoveryBuilder.builder(ServicePayload.class)
          .client(client)
          .basePath(SERVICE_PATH)
          .serializer(new ServicePayloadSerializer())
          .build();
      discovery.start();
    } catch (IOException e) {
      throw new ServiceDiscoveryClientException(e);
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e);
    }
    started = true;
  }

  /**
   * Returns the instance of ServiceDiscovery object.
   *
   * @return instance of service discovery.
   */
  public ServiceDiscovery<ServicePayload> getServiceDiscovery() {
    return discovery;
  }

  /**
   * Registers a given service along with the payload to stored a KV under the
   * service registration node.
   *
   * @param name of the service to be registered
   * @param port on which the services are provided.
   * @param payload associated with the service.
   * @throws ServiceDiscoveryClientException
   */
  public void register(String name, String address, int port, ServicePayload payload)
      throws ServiceDiscoveryClientException {
    name = Preconditions.checkNotNull(name);
    port = Preconditions.checkNotNull(port);

    if(! started) {
      throw new
        ServiceDiscoveryClientException("ServiceDiscoveryClient#start has " +
                                          "not been called.");
    }

    try {
      ServiceInstance<ServicePayload> instance =
          ServiceInstance.<ServicePayload>builder()
            .payload(payload)
            .name(name)
            .address(address)
            .port(port)
            .serviceType(ServiceType.DYNAMIC)
            .build();
      discovery.registerService(instance);
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e);
    }
  }

  /**
   * Unregisters a service from the Service discovery system.
   *
   * @param name  of the service to be unregistered
   * @throws ServiceDiscoveryClientException
   */
  public void unregister(String name) throws ServiceDiscoveryClientException {
    name = Preconditions.checkNotNull(name);
    if(! started) {
      throw new ServiceDiscoveryClientException("ServiceDiscoveryClient#start has not been called.");
    }
    try {
      ServiceInstance<ServicePayload> instance =
        ServiceInstance.<ServicePayload>builder()
          .name(name)
          .serviceType(ServiceType.DYNAMIC)
          .build();
      discovery.unregisterService(instance);
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e);
    }
  }

  /**
   * Returns number of providers for a service
   *
   * @param name of the service
   * @return count of service providers for the name requested.
   * @throws ServiceDiscoveryClientException
   */
  public int getProviderCount(String name) throws ServiceDiscoveryClientException {
    try {
      return discovery.queryForInstances(name).size();
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e);
    }
  }

  /**
   * Handy utility for retrieving an instance.
   *
   * @param strategy for selecting a provider.
   * @return service instance object.
   * @throws ServiceDiscoveryClientException
   */
  public synchronized ServiceInstance<ServicePayload>
    getInstance(String serviceName, ProviderStrategy<ServicePayload> strategy)
      throws ServiceDiscoveryClientException{

    try {
      // get the providers that have already registered.
      ServiceProvider provider = getServiceProvider(serviceName);

      // If there are no instances available, then return null.
      if(provider.getInstances().size() < 1) {
        return null;
      }

      // Applying the strategy provided, get one instance to connect to.
      return strategy.getInstance(provider);
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e.getMessage());
    }
  }

  /**
   * Closes all the closeables registered.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    if(client != null) {
      client.close();
    }
    if(discovery != null) {
      discovery.close();
    }
  }

  /**
   * Returns an instance of service provider
   *
   * @param name of the service
   * @return ServiceProvider instance.
   * @throws ServiceDiscoveryClientException
   */
  public ServiceProvider getServiceProvider(String name)
    throws ServiceDiscoveryClientException {
    return new ServiceProvider(name, discovery);
  }

  public static class ServiceProvider
    implements InstanceProvider<ServicePayload> {

    private final Collection<ServiceInstance<ServicePayload>> instances;

    public ServiceProvider(String name, ServiceDiscovery<ServicePayload> discovery)
        throws ServiceDiscoveryClientException {
      try {
        instances = discovery.queryForInstances(name);
      } catch (Exception e) {
        throw new ServiceDiscoveryClientException(e);
      }
    }
    @Override
    public List<ServiceInstance<ServicePayload>> getInstances()
      throws Exception {
      return ImmutableList.copyOf(instances);
    }
  }

}
