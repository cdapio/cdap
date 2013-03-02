/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.server;


import com.continuuity.common.conf.CConfiguration;
import com.continuuity.passport.Constants;
import com.continuuity.passport.dal.db.JDBCAuthrozingRealm;
import com.continuuity.passport.http.modules.PassportGuiceServletContextListener;
import com.google.inject.servlet.GuiceFilter;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.management.MBeanContainer;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Jetty based Http Servers
 */
public class PassportHttpServer {

  private final int gracefulShutdownTime;
  private final int port;
  private final int maxThreads;

  private final Map<String, String> configuration;
  private static final Logger LOG = LoggerFactory.getLogger(PassportHttpServer.class);

  public PassportHttpServer(int port, Map<String, String> configuration,
                            int maxThreads, int gracefulShutdownTime) {
    this.port = port;
    this.configuration = configuration;
    this.maxThreads = maxThreads;
    this.gracefulShutdownTime = gracefulShutdownTime;
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
      LOG.info(String.format("Port: %d",port));
      LOG.info(String.format("Threads: %d",maxThreads));
      for(Map.Entry<String,String> e: configuration.entrySet()){
        LOG.info(String.format("Config %s : %s", e.getKey(),e.getValue()));
      }

      server.start();
      server.join();
      LOG.info("Server started Successfully");

    } catch (Exception e) {
      LOG.error("Error while starting server");
      LOG.error(e.getMessage());
    }
  }

  public static void main(String[] args) {

    Map<String, String> config = new HashMap<String, String>();

    //TODO: Pass in the CConfiguration object directly instead of copying it in to a HashMap
    CConfiguration conf = CConfiguration.create();

    int port = conf.getInt(Constants.CFG_SERVER_PORT, Constants.DEFAULT_SERVER_PORT);
    int gracefulShutdownTime = conf.getInt(Constants.CFG_GRACEFUL_SHUTDOWN_TIME,Constants.DEFAULT_GRACEFUL_SHUTDOWN_TIME);
    int maxThreads = conf.getInt(Constants.CFG_HTTP_MAX_THREADS,Constants.DEFAULT_HTTP_MAX_THREADS);

    config.put(Constants.CFG_JDBC_TYPE, conf.get(Constants.CFG_JDBC_TYPE,Constants.DEFAULT_JDBC_TYPE));
    config.put(Constants.CFG_JDBC_CONNECTION_STRING,conf.get(Constants.CFG_JDBC_CONNECTION_STRING,
                                           Constants.DEFAULT_JDBC_CONNECTION_STRING));
    config.put(Constants.CFG_PROFANE_WORDS_FILE_PATH,conf.get(Constants.CFG_PROFANE_WORDS_FILE_PATH,
                                                              Constants.DEFAULT_PROFANE_WORDS_FILE_PATH));

    Realm realm = new JDBCAuthrozingRealm(config);

    org.apache.shiro.mgt.SecurityManager securityManager = new DefaultSecurityManager(realm);
    SecurityUtils.setSecurityManager(securityManager);

    PassportHttpServer server = new PassportHttpServer(port, config, maxThreads,gracefulShutdownTime);
    server.start();


  }
}
