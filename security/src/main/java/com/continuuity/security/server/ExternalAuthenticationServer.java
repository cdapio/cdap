package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Jetty service for External Authentication.
 */
public class ExternalAuthenticationServer extends AbstractExecutionThreadService {
  private final int port;
  private final int maxThreads;
  private final HashMap<String, Object> handlers;
  private final DiscoveryService discoveryService;
  private final CConfiguration configuration;
  private Cancellable serviceCancellable;
  private final GrantAccessToken grantAccessToken;
  private final AbstractAuthenticationHandler authenticationHandler;
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAuthenticationServer.class);
  private Server server;
  private InetAddress address;

  /**
   * Constants for a valid JSON response.
   */
  public static final class ResponseFields {
    public static final String TOKEN_TYPE = "token_type";
    public static final String TOKEN_TYPE_BODY = "Bearer";
    public static final String ACCESS_TOKEN = "access_token";
    public static final String EXPIRES_IN = "expires_in";
  }

  /**
   * Constants for Handler types.
   */
  public static final class HandlerType {
    public static final String AUTHENTICATION_HANDLER = "AuthenticationHandler";
    public static final String GRANT_TOKEN_HANDLER = "GrantTokenHandler";
  }

  @Inject
  public ExternalAuthenticationServer(CConfiguration configuration, DiscoveryService discoveryService,
                                      @Named("security.handlers") HashMap handlers) {
    this.port = configuration.getInt(Constants.Security.AUTH_SERVER_PORT);
    this.maxThreads = configuration.getInt(Constants.Security.MAX_THREADS);
    this.handlers = handlers;
    this.discoveryService = discoveryService;
    this.configuration = configuration;
    this.grantAccessToken = (GrantAccessToken) handlers.get(HandlerType.GRANT_TOKEN_HANDLER);
    this.authenticationHandler = (AbstractAuthenticationHandler) handlers.get(HandlerType.AUTHENTICATION_HANDLER);
  }

  /**
   * Get the InetSocketAddress of the server.
   * @return InetSocketAddress of server.
   */
  public InetSocketAddress getSocketAddress() {
    return new InetSocketAddress(address, port);
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
        return new InetSocketAddress(address, port);
      }
    });
    server.start();
  }

  @Override
  protected void startUp() {
    try {
      server = new Server();

      try {
        address = InetAddress.getByName(configuration.get(Constants.Security.AUTH_SERVER_ADDRESS));
      } catch (UnknownHostException e) {
        LOG.error("Error finding host to connect to.", e);
        throw Throwables.propagate(e);
      }

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMaxThreads(maxThreads);
      server.setThreadPool(threadPool);

      initHandlers();

      ServletContextHandler context = new ServletContextHandler();
      context.setServer(server);
      context.addServlet(HttpServletDispatcher.class, "/");
      context.addEventListener(new AuthenticationGuiceServletContextListener(handlers, configuration));
      context.setSecurityHandler(authenticationHandler);

      SelectChannelConnector connector = new SelectChannelConnector();
      connector.setHost(address.getCanonicalHostName());
      connector.setPort(port);

      if (configuration.getBoolean(Constants.Security.SSL_ENABLED, false)) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        String keystorePath = configuration.get(Constants.Security.SSL_KEYSTORE_PATH);
        String keyStorePassword = configuration.get(Constants.Security.SSL_KEYSTORE_PASSWORD);
        if (keystorePath == null || keyStorePassword == null) {
          String errorMessage = String.format("Keystore is not configured correctly. Have you configured %s and %s?",
                                              Constants.Security.SSL_KEYSTORE_PATH,
                                              Constants.Security.SSL_KEYSTORE_PASSWORD);
          throw Throwables.propagate(new RuntimeException(errorMessage));
        }
        sslContextFactory.setKeyStorePath(keystorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);

        SslSelectChannelConnector sslConnector = new SslSelectChannelConnector(sslContextFactory);
        int sslPort = configuration.getInt(Constants.Security.AUTH_SERVER_SSL_PORT);
        sslConnector.setHost(address.getCanonicalHostName());
        sslConnector.setPort(sslPort);
        connector.setConfidentialPort(sslPort);
        server.setConnectors(new Connector[]{connector, sslConnector});
      } else {
        server.setConnectors(new Connector[]{connector});
      }

      server.setHandler(context);
    } catch (Exception e) {
      LOG.error("Error while starting server.");
      LOG.error(e.getMessage());
    }
  }

  /**
   * Initializes the handlers.
   * @return
   */
  protected void initHandlers() throws Exception {
    authenticationHandler.init();
    grantAccessToken.init();
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
      grantAccessToken.destroy();
    } catch (Exception e) {
      LOG.error("Error stopping ExternalAuthenticationServer.");
      LOG.error(e.getMessage());
    }
  }
}
