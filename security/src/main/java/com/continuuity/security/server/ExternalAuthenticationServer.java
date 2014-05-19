package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.SecurityModules;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Jetty service for External Authentication.
 */
public class ExternalAuthenticationServer extends AbstractExecutionThreadService {
  private final int port;
  private final int maxThreads;
  private final HandlerList handlers;
  private final DiscoveryService discoveryService;
  private final CConfiguration configuration;
  private Cancellable serviceCancellable;
  private InetSocketAddress socketAddress;
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAuthenticationServer.class);
  private Server server;

  /**
   * Constants for a valid JSON response.
   */
  protected static final class ResponseFields {
    protected static final String TOKEN_TYPE = "token_type";
    protected static final String TOKEN_TYPE_BODY = "Bearer";
    protected static final String ACCESS_TOKEN = "access_token";
    protected static final String EXPIRES_IN = "expires_in";
  }

  @Inject
  public ExternalAuthenticationServer(CConfiguration configuration, DiscoveryService discoveryService,
                                      @Named("security.handlers") HandlerList handlers) {
    this.port = configuration.getInt(Constants.Security.AUTH_SERVER_PORT);
    this.maxThreads = configuration.getInt(Constants.Security.MAX_THREADS);
    this.handlers = handlers;
    this.discoveryService = discoveryService;
    this.configuration = configuration;
  }

  /**
   * Get the InetSocketAddress of the server.
   * @return InetSocketAddress of server.
   */
  public InetSocketAddress getSocketAddress() {
    return this.socketAddress;
  }

  @Override
  protected void run() throws Exception {
    serviceCancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.EXTERNAL_AUTHENTICATION;
      }

      @Override
      public InetSocketAddress getSocketAddress() throws RuntimeException {
        InetAddress address;
        try {
          address = InetAddress.getByName(server.getConnectors()[0].getHost());
        } catch (UnknownHostException e) {
          LOG.error("Error finding host to connect to.", e);
          throw Throwables.propagate(e);
        }
        socketAddress = new InetSocketAddress(address, port);
        return socketAddress;
      }
    });
    server.start();
  }

  @Override
  protected void startUp() {
    try {
      server = new Server();

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMaxThreads(maxThreads);
      server.setThreadPool(threadPool);

      ContextHandler context = new ContextHandler();
      context.setContextPath("*");
      context.setHandler(handlers);

      SelectChannelConnector connector;

      if (configuration.getBoolean(Constants.Security.SSL_ENABLED, false)) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        String keystorePath = configuration.get("security.server.ssl.keystore.path");
        String keyStorePassword = configuration.get("security.server.ssl.keystore.password");
        if (keystorePath == null || keyStorePassword == null) {
          throw Throwables.propagate(new RuntimeException("Keystore not configured correctly"));
        }
        sslContextFactory.setKeyStorePath(keystorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
//        sslContextFactory.setTrustAll(true);
//        sslContextFactory.setValidateCerts(false);

        connector = new SslSelectChannelConnector(sslContextFactory);
      } else {
        connector = new SelectChannelConnector();
      }

      connector.setPort(port);
      server.setConnectors(new Connector[]{connector});
      server.setHandler(context);
    } catch (Exception e) {
      LOG.error("Error while starting server.");
      LOG.error(e.getMessage());
    }
  }

  @Override
  protected Executor executor() {
    final AtomicInteger id = new AtomicInteger();
    return new Executor() {
      @Override
      public void execute(Runnable runnable) {
        new Thread(runnable, String.format("ExternalAuthenticationService-%d", id.incrementAndGet())).start();
      }
    };
  }

  @Override
  protected void triggerShutdown() {
    try {
      serviceCancellable.cancel();
      server.stop();
    } catch (Exception e) {
      LOG.error("Error stopping ExternalAuthenticationServer.");
      LOG.error(e.getMessage());
    }
  }

  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new IOModule(), new SecurityModules().getInMemoryModules(), new DiscoveryRuntimeModule().getInMemoryModules(),
                                             new ConfigModule());
    ExternalAuthenticationServer server = injector.getInstance(ExternalAuthenticationServer.class);
    server.startAndWait();
  }
}
