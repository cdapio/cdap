/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * AppFabric Server that implements {@link AbstractExecutionThreadService}.
 */
public class AppFabricServer extends AbstractExecutionThreadService {
  private static final int THREAD_COUNT = 2;

  private final AppFabricService.Iface service;
  private final int port;
  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final SchedulerService schedulerService;

  private TThreadedSelectorServer server;
  private ExecutorService executor;
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServer.class);
  /**
   * Construct the AppFabricServer with service factory and configuration coming from guice injection.
   */
  @Inject
  public AppFabricServer(AppFabricServiceFactory serviceFactory, CConfiguration configuration,
                         DiscoveryService discoveryService, SchedulerService schedulerService,
                         @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.schedulerService = schedulerService;
    this.service = serviceFactory.create(schedulerService);
    this.port = configuration.getInt(Constants.AppFabric.SERVER_PORT,
                                     Constants.AppFabric.DEFAULT_SERVER_PORT);
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {

    executor = Executors.newFixedThreadPool(THREAD_COUNT, Threads.createDaemonThreadFactory("app-fabric-server-%d"));
    schedulerService.start();
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

    TThreadedSelectorServer.Args options = new TThreadedSelectorServer.Args(new TNonblockingServerSocket(socketAddress))
      .executorService(executor)
      .processor(new AppFabricService.Processor<AppFabricService.Iface>(service))
      .workerThreads(THREAD_COUNT);
    options.maxReadBufferBytes = Constants.Thrift.DEFAULT_MAX_READ_BUFFER;
    server = new TThreadedSelectorServer(options);
  }

  /**
   * Runs the AppFabricServer.
   * <p>
   *   It's run on a different thread.
   * </p>
   */
  @Override
  protected void run() throws Exception {
    server.serve();
  }

  /**
   * Invoked during shutdown of the thread.
   */
  protected void triggerShutdown() {
    schedulerService.stopAndWait();
    executor.shutdownNow();
    server.stop();
  }

  public AppFabricService.Iface getService() {
    return service;
  }
}
