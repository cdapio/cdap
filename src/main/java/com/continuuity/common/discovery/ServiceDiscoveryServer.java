package com.continuuity.common.discovery;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryUntilElapsed;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.server.entity.JsonServiceInstanceMarshaller;
import com.netflix.curator.x.discovery.server.entity.JsonServiceInstancesMarshaller;
import com.netflix.curator.x.discovery.server.entity.JsonServiceNamesMarshaller;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import javax.ws.rs.core.Application;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Service discovery server that bridges non-java and/or legacy application
 * with curator service discovery system. It exposes a RESTful web service
 * for registering, removing or querying the services.
 *
 * The implementation uses Jersey for JAX-RS implementation and Jetty as
 * the container.
 */
public class ServiceDiscoveryServer {
  private Server server;
  private final Application application;
  private final CuratorFramework framework;
  private final ServiceDiscovery<ServicePayload> serviceDiscovery;
  private final List<Closeable> closeables = Lists.newArrayList();
  private static int ZK_MAX_CONNECT_TIME_IN_MS = 30 * 1000;
  private static int ZK_INTER_CONNECT_INTERVAL_IN_MS = 100;
  private final JsonServiceNamesMarshaller serviceNamesMarshaller;
  private final JsonServiceInstanceMarshaller<ServicePayload>
    serviceInstanceMarshaller;
  private final JsonServiceInstancesMarshaller<ServicePayload>
    serviceInstancesMarshaller;
  private final ServicePayloadContext context;

  public ServiceDiscoveryServer(CConfiguration configuration) throws Exception {
    Preconditions.checkNotNull(configuration);

    // Retrieve zookeeper configuration.
    String zookeeper = configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE,
                                         Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);

    // Create a curator framework handle.
    framework = CuratorFrameworkFactory.newClient(
      zookeeper,
      new RetryUntilElapsed(ZK_MAX_CONNECT_TIME_IN_MS,
                            ZK_INTER_CONNECT_INTERVAL_IN_MS)
    );
    framework.start();

    // Add it to closeables.
    closeables.add(framework);

    // Create a service discovery that allocated ServiceProviders.
    serviceDiscovery =
      ServiceDiscoveryBuilder.builder(ServicePayload.class)
      .client(framework)
      .basePath(ServiceDiscoveryClient.SERVICE_PATH)
      .build();

    // Start service discovery.
    serviceDiscovery.start();

    // Add the service discovery to closeable.
    closeables.add(serviceDiscovery);

    // Create mapped discovery context.
    context = new ServicePayloadContext(
      serviceDiscovery,
      new RandomStrategy<ServicePayload>(), 1000
    );

    serviceNamesMarshaller = new JsonServiceNamesMarshaller();
    serviceInstanceMarshaller = new JsonServiceInstanceMarshaller
      <ServicePayload>(context);
    serviceInstancesMarshaller = new JsonServiceInstancesMarshaller
      <ServicePayload>(context);

    application = new DefaultResourceConfig() {
      @Override
      public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = Sets.newHashSet();
        classes.add(MapDiscoveryResource.class);
        return classes;
      }

      @Override
      public Set<Object> getSingletons() {
        Set<Object> singletons = Sets.newHashSet();
        singletons.add(context);
        singletons.add(serviceNamesMarshaller);
        singletons.add(serviceInstanceMarshaller);
        singletons.add(serviceInstancesMarshaller);
        return singletons;
      }
    };
  }

  public JsonServiceNamesMarshaller getServiceNamesMarshaller() {
    return serviceNamesMarshaller;
  }

  public ServicePayloadContext getContext() {
    return context;
  }

  public JsonServiceInstanceMarshaller<ServicePayload>
    getServiceInstanceMarshaller() {
    return serviceInstanceMarshaller;
  }

  public JsonServiceInstancesMarshaller<ServicePayload>
    getServiceInstancesMarshaller() {
    return serviceInstancesMarshaller;
  }

  public void start(int port) throws Exception {
    Preconditions.checkArgument(port > 1024,
                                "Port should be greater than 1024.");
    ServletContainer container = new ServletContainer(application);
    server = new Server(port);
    Context root = new Context(server, "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(container), "/*");
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
    server.join();
    Collections.reverse(closeables);
    for(Closeable c : closeables) {
      Closeables.closeQuietly(c);
    }
  }
}
