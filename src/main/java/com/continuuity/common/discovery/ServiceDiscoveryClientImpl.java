package com.continuuity.common.discovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceType;
import com.netflix.curator.x.discovery.details.InstanceProvider;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * A Service registration client that supports registering a service
 * with a central service discovery service. This essentially is a wrapper
 * around the Netflix curator for managing states.
 */
public class ServiceDiscoveryClientImpl implements Closeable, ServiceDiscoveryClient {
  private final String connectionString;
  private final List<Closeable> closeables;
  private static ServiceDiscovery<Map> discovery = null;
  private static final String servicePath = "/continuuity/system/services";
  private static final int sessionTimeout = 10*1000;
  private static final int connectionTimeout = 5*1000;
  private static final int numberOfRetry = 5;
  private static final int timeBetweenRetries = 10;
  private static boolean started = false;


  /**
   * Constructs a service registration using an connection string.
   *
   * @param connectionString the connect string to ZK ensemble.
   * @throws IOException
   */
  public ServiceDiscoveryClientImpl(String connectionString) {
    connectionString = Preconditions.checkNotNull(connectionString);
    this.connectionString = connectionString;
    closeables = Lists.newArrayList();
  }

  /**
   * Starts the curator framework and discovery mechanism.
   * @throws ServiceDiscoveryClientException
   */
  public void start() throws ServiceDiscoveryClientException {
    CuratorFramework client;
    try {
      // Create a curator client.
      client = CuratorFrameworkFactory.newClient(connectionString, sessionTimeout,
          connectionTimeout, new RetryNTimes(numberOfRetry, timeBetweenRetries));
      closeables.add(client);
      client.start();

      // Create a service discovery that allocated ServiceProviders.
      discovery = ServiceDiscoveryBuilder.builder(Map.class)
          .client(client)
          .basePath(servicePath)
          .serializer(new JsonInstanceSerializer<Map>(Map.class))
          .build();
      discovery.start();
      closeables.add(discovery);
    } catch (IOException e) {
      throw new ServiceDiscoveryClientException(e);
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e);
    }
    started = true;
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
  public void register(String name, int port, Map<String, String> payload)
      throws ServiceDiscoveryClientException {
    name = Preconditions.checkNotNull(name);
    port = Preconditions.checkNotNull(port);

    if(! started) {
      throw new ServiceDiscoveryClientException("ServiceDiscoveryClientImpl#start has not been called.");
    }

    try {
      ServiceInstance<Map> instance =
          ServiceInstance.<Map>builder()
            .payload(payload)
            .name(name)
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
      throw new ServiceDiscoveryClientException("ServiceDiscoveryClientImpl#start has not been called.");
    }
    try {
      ServiceInstance<Map> instance =
        ServiceInstance.<Map>builder()
          .name(name)
          .serviceType(ServiceType.DYNAMIC)
          .build();
      discovery.unregisterService(instance);
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e);
    }
  }

  /**
   *  Returns number of providers for a service
   * @param name of the service
   * @return count of service providers for the name requested.
   * @throws ServiceDiscoveryClientException
   */
  public int getProviderCount(String name) throws ServiceDiscoveryClientException {
    int count = 0;
    try {
      count =  discovery.queryForInstances(name).size();
    } catch (Exception e) {
      throw new ServiceDiscoveryClientException(e);
    }
    return count;
  }

  /**
   * Closes all the closeables registered.
   * @throws IOException
   */
  public void close() throws IOException {
    Collections.reverse(closeables);
    for(Closeable c : closeables) {
      Closeables.close(c, false);
    }
  }

  public static class ServiceProvider implements InstanceProvider<Map> {
    Collection<ServiceInstance<Map>> instances;

    public ServiceProvider(final String name)
        throws ServiceDiscoveryClientException {
      try {
        instances = discovery.queryForInstances(name);
      } catch (Exception e) {
        throw new ServiceDiscoveryClientException(e);
      }
    }

    @Override
    public List<ServiceInstance<Map>> getInstances() throws Exception {
      return new ArrayList<ServiceInstance<Map>>(instances);
    }
  }

}
