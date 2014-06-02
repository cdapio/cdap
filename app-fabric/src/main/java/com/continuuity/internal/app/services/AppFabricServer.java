/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * AppFabric Server.
 */
public final class AppFabricServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServer.class);

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final SchedulerService schedulerService;
  private final ProgramRuntimeService programRuntimeService;

  private NettyHttpService httpService;
  private Set<HttpHandler> handlers;
  private MetricsCollectionService metricsCollectionService;
  private CConfiguration configuration;
  private LogAppenderInitializer logAppenderInitializer;

  /**
   * Construct the AppFabricServer with service factory and configuration coming from guice injection.
   */
  @Inject
  public AppFabricServer(CConfiguration configuration, DiscoveryService discoveryService,
                         SchedulerService schedulerService,
                         @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                         @Named("appfabric.http.handler") Set<HttpHandler> handlers,
                         @Nullable MetricsCollectionService metricsCollectionService,
                         ProgramRuntimeService programRuntimeService, LogAppenderInitializer logAppenderInitializer) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.schedulerService = schedulerService;
    this.handlers = handlers;
    this.configuration = configuration;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.logAppenderInitializer = logAppenderInitializer;
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    logAppenderInitializer.initialize();
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.APP_FABRIC_HTTP));
    schedulerService.start();
    programRuntimeService.start();

    // Run http service on random port
    httpService = NettyHttpService.builder()
      .setHost(hostname.getCanonicalHostName())
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Service.APP_FABRIC_HTTP)))
      .addHttpHandlers(handlers)
      .setConnectionBacklog(configuration.getInt(Constants.Gateway.BACKLOG_CONNECTIONS,
                                                 Constants.Gateway.DEFAULT_BACKLOG))
      .setExecThreadPoolSize(configuration.getInt(Constants.Gateway.EXEC_THREADS,
                                                  Constants.Gateway.DEFAULT_EXEC_THREADS))
      .setBossThreadPoolSize(configuration.getInt(Constants.Gateway.BOSS_THREADS,
                                                  Constants.Gateway.DEFAULT_BOSS_THREADS))
      .setWorkerThreadPoolSize(configuration.getInt(Constants.Gateway.WORKER_THREADS,
                                                    Constants.Gateway.DEFAULT_WORKER_THREADS))
      .build();

    // Add a listener so that when the service started, register with service discovery.
    // Remove from service discovery when it is stopped.
    httpService.addListener(new ServiceListenerAdapter() {

      private Cancellable cancellable;

      @Override
      public void running() {
        final InetSocketAddress socketAddress = httpService.getBindAddress();
        LOG.info("AppFabric HTTP Service started at {}", socketAddress);

        // When it is running, register it with service discovery
        cancellable = discoveryService.register(new Discoverable() {

          @Override
          public String getName() {
            return Constants.Service.APP_FABRIC_HTTP;
          }

          @Override
          public InetSocketAddress getSocketAddress() {
            return socketAddress;
          }
        });
      }

      @Override
      public void terminated(State from) {
        LOG.info("AppFabric HTTP service stopped.");
        if (cancellable != null) {
          cancellable.cancel();
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.info("AppFabric HTTP service stopped with failure.", failure);
        if (cancellable != null) {
          cancellable.cancel();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    httpService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
    programRuntimeService.stopAndWait();
    schedulerService.stopAndWait();
  }
}
