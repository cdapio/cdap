/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.server;


import com.continuuity.common.conf.CConfiguration;
import com.continuuity.passport.Constants;
import com.continuuity.passport.http.modules.PassportGuiceServletContextListener;
import com.google.common.base.Preconditions;
import com.google.inject.servlet.GuiceFilter;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.management.MBeanContainer;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;

/**
 * Jetty based Http Server.
 */
public class PassportHttpServer {

  private final int gracefulShutdownTime;
  private final int port;
  private final int maxThreads;

  private final CConfiguration configuration;
  private static final Logger LOG = LoggerFactory.getLogger(PassportHttpServer.class);

  public PassportHttpServer(CConfiguration configuration)  {
    Preconditions.checkNotNull(configuration, "Configuration should not be null");

    this.port = configuration.getInt(Constants.CFG_SERVER_PORT, Constants.DEFAULT_SERVER_PORT);
    this.gracefulShutdownTime = configuration.getInt(Constants.CFG_GRACEFUL_SHUTDOWN_TIME,
                                                     Constants.DEFAULT_GRACEFUL_SHUTDOWN_TIME);
    this.maxThreads = configuration.getInt(Constants.CFG_HTTP_MAX_THREADS, Constants.DEFAULT_HTTP_MAX_THREADS);

    this.configuration = configuration;
  }

  private void start() {
    try {
     Server server = new Server();
      server.setStopAtShutdown(true);
      server.setGracefulShutdown(gracefulShutdownTime);

      Context context = new Context(server, "/", Context.SESSIONS);
      context.addEventListener(new PassportGuiceServletContextListener(configuration));
      context.addServlet(DefaultServlet.class, "/");
      context.addFilter(GuiceFilter.class, "/*", 0);

      // Use the connector to bind Jetty to local host
      Connector connector = new SelectChannelConnector();
      connector.setHost("localhost");
      connector.setPort(port);
      server.addConnector(connector);

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMaxThreads(maxThreads);
      server.setThreadPool(threadPool);

      //JMX jetty
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
      server.getContainer().addEventListener(mBeanContainer);
      mBeanContainer.start();

      LOG.info("Starting the server with the following parameters");
      LOG.info(String.format("Port: %d", port));
      LOG.info(String.format("Threads: %d", maxThreads));

      server.start();
      server.join();
      LOG.info("Server started Successfully");

    } catch (Exception e) {
      LOG.error("Error while starting server");
      LOG.error(e.getMessage());
    }
  }

  public static void main(String[] args) {
    CConfiguration configuration = CConfiguration.create();

    PassportHttpServer server = new PassportHttpServer(configuration);
    server.start();
  }
}
