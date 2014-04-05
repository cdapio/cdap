package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.Constants;

import com.continuuity.security.guice.SecurityModule;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jetty service for External Authentication.
 */
public class ExternalAuthenticationServer extends AbstractExecutionThreadService {
  private final CConfiguration configuration;
  private final int port;
  private final int maxThreads;
  private HandlerList handlers;
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAuthenticationServer.class);
  private Server server;

  @Inject
  public ExternalAuthenticationServer(CConfiguration conf,
                                        GrantAccessTokenHandler grantAccessTokenHandler,
                                        BasicAuthenticationHandler authenticationHandler) {
    this.configuration = conf;
    this.port = configuration.getInt(Constants.AUTH_SERVER_PORT, Constants.DEFAULT_AUTH_SERVER_PORT);
    this.maxThreads = configuration.getInt(Constants.MAX_THREADS, Constants.DEFAULT_MAX_THREADS);
    this.handlers = new HandlerList();
    this.handlers.setHandlers(new Handler[] {authenticationHandler, grantAccessTokenHandler});
  }

  @Override
  protected void run() throws Exception {
    server.start();
  }

  @Override
  protected void startUp() {
    try {
      server = new Server();

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMaxThreads(maxThreads);
      server.setThreadPool(threadPool);

      Connector connector = new SelectChannelConnector();
      connector.setPort(port);
      server.setConnectors(new Connector[]{connector});

      server.setHandler(handlers);
    } catch (Exception e) {
      LOG.error("Error while starting server.");
      LOG.error(e.getMessage());
    }
  }

  @Override
  protected void triggerShutdown() {
    try {
      server.stop();
    } catch (Exception e) {
      LOG.error("Error stopping ExternalAuthenticationServer.");
      LOG.error(e.getMessage());
    }
  }
}
