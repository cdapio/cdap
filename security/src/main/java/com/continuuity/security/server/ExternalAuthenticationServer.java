package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Jetty service for External Authentication.
 */
public class ExternalAuthenticationServer extends AbstractExecutionThreadService {
  private final int port;
  private final int maxThreads;
  private final HandlerList handlers;
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
  public ExternalAuthenticationServer(CConfiguration configuration, @Named("security.handlers") HandlerList handlers) {
    this.port = configuration.getInt(Constants.Security.AUTH_SERVER_PORT, Constants.Security.DEFAULT_AUTH_SERVER_PORT);
    this.maxThreads = configuration.getInt(Constants.Security.MAX_THREADS, Constants.Security.DEFAULT_MAX_THREADS);
    this.handlers = handlers;
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
      server.stop();
    } catch (Exception e) {
      LOG.error("Error stopping ExternalAuthenticationServer.");
      LOG.error(e.getMessage());
    }
  }
}
