/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * AppFabric Server that implements {@link AbstractExecutionThreadService}.
 */
public class AppFabricServer extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServer.class);
  private static final int THREAD_COUNT = 2;


  private final int port;
  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final SchedulerService schedulerService;
  private final ProgramRuntimeService programRuntimeService;

  private NettyHttpService httpService;
  private ExecutorService executor;
  private Set<HttpHandler> handlers;
  private MetricsCollectionService metricsCollectionService;
  private CConfiguration configuration;

  /**
   * Construct the AppFabricServer with service factory and configuration coming from guice injection.
   */
  @Inject
  public AppFabricServer(CConfiguration configuration, DiscoveryService discoveryService,
                         SchedulerService schedulerService,
                         @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                         @Named("appfabric.http.handler") Set<HttpHandler> handlers,
                         @Nullable MetricsCollectionService metricsCollectionService,
                         ProgramRuntimeService programRuntimeService) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.schedulerService = schedulerService;
    this.port = configuration.getInt(Constants.AppFabric.SERVER_PORT, Constants.AppFabric.DEFAULT_SERVER_PORT);
    this.handlers = handlers;
    this.configuration = configuration;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {

    executor = Executors.newFixedThreadPool(THREAD_COUNT, Threads.createDaemonThreadFactory("app-fabric-server-%d"));
    schedulerService.start();
    programRuntimeService.start();

    // Register with discovery service.
    InetSocketAddress socketAddress = new InetSocketAddress(hostname, port);
    InetAddress address = socketAddress.getAddress();
    if (address.isAnyLocalAddress()) {
      address = InetAddress.getLocalHost();
    }
    final InetSocketAddress finalSocketAddress = new InetSocketAddress(address, port);

    discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.APP_FABRIC;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return finalSocketAddress;
      }
    });

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

  }

  /**
   * Runs the AppFabricServer.
   * <p>
   *   It's run on a different thread.
   * </p>
   */
  @Override
  protected void run() throws Exception {
    httpService.startAndWait();
    final int httpPort = httpService.getBindAddress().getPort();
    final InetSocketAddress socketAddress = new InetSocketAddress(hostname, httpPort);
    final InetAddress httpAddress = socketAddress.getAddress();
    discoveryService.register(new Discoverable() {
      final InetSocketAddress finalHttpSocketAddress = new InetSocketAddress(httpAddress, httpPort);

      @Override
      public String getName() {
        return Constants.Service.APP_FABRIC_HTTP;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return finalHttpSocketAddress;
      }
    });
  }

  /**
   * Invoked during shutdown of the thread.
   */
  protected void triggerShutdown() {
    programRuntimeService.stopAndWait();
    schedulerService.stopAndWait();
    executor.shutdownNow();
    httpService.stopAndWait();
  }

}
