/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;

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

  /**
   * Construct the AppFabricServer with service factory and configuration coming from factory.
   *
   * @param service
   * @param configuration
   */
  @Inject
  public AppFabricServer(AppFabricService.Iface service, CConfiguration configuration) {
    this.conf = configuration;
    executor = Executors.newFixedThreadPool(THREAD_COUNT);
    this.service = service;
    port = configuration.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT, Constants.DEFAULT_APP_FABRIC_SERVER_PORT);
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    options = new TThreadedSelectorServer.Args(new TNonblockingServerSocket(port))
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
