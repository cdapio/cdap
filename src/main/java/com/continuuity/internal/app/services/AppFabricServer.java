/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.continuuity.discovery.Discoverable;
import com.continuuity.discovery.DiscoveryService;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * AppFabric Server that implements {@link AbstractExecutionThreadService}.
 */
public class AppFabricServer extends AbstractExecutionThreadService {
  private static final int THREAD_COUNT = 2;

  private CConfiguration conf;
  private ExecutorService executor;
  private AppFabricService.Iface service;
  private TThreadedSelectorServer server;
  private Thread runnerThread;
  private TThreadedSelectorServer.Args options;
  private int port;
  private final DiscoveryService discoveryService;
  private String hostname;

  /**
   * Construct the AppFabricServer with service factory and configuration coming from factory.
   *
   * @param service
   * @param configuration
   */
  @Inject
  public AppFabricServer(AppFabricService.Iface service, CConfiguration configuration,
                         DiscoveryService discoveryService) {
    this.conf = configuration;
    executor = Executors.newFixedThreadPool(THREAD_COUNT);
    this.service = service;
    this.discoveryService = discoveryService;
    port = configuration.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT, Constants.DEFAULT_APP_FABRIC_SERVER_PORT);
    hostname = configuration.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS, Constants.DEFAULT_APP_FABRIC_SERVER_ADDRESS);
  }

  /**
   * @return Discoverable Object
   */
  private Discoverable createDiscoverable() {
    return new Discoverable() {
      @Override
      public String getName() {
        return "app-fabric-service";
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(port);
      }
    };
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {

    // Register with discovery service.
    discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return "app.fabric.service";
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        try {
          return new InetSocketAddress(InetAddress.getByName(hostname), port);
        } catch (UnknownHostException e) {
          throw Throwables.propagate(e);
        }
      }
    });

    options = new TThreadedSelectorServer.Args(new TNonblockingServerSocket(new InetSocketAddress(hostname, port)))
      .executorService(executor)
      .processor(new AppFabricService.Processor(service))
      .workerThreads(THREAD_COUNT);
    options.maxReadBufferBytes = Constants.DEFAULT_MAX_READ_BUFFER;
    runnerThread = Thread.currentThread();

    // Start the flow metrics reporter.
    OverlordMetricsReporter.enable(1, TimeUnit.SECONDS, conf);
  }

  /**
   * Runs the AppFabricServer.
   * <p>
   *   It's run on a different thread.
   * </p>
   */
  @Override
  protected void run() throws Exception {
    server = new TThreadedSelectorServer(options);
    server.serve();
  }

  /**
   * Invoked during shutdown of the thread.
   */
  protected void triggerShutdown() {
    runnerThread.interrupt();
  }
}
