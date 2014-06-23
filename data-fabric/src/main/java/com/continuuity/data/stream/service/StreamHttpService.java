/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.net.InetSocketAddress;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A Http service endpoint that host the stream handler.
 */
public final class StreamHttpService extends AbstractIdleService {

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final StreamCoordinator streamCoordinator;
  private final StreamFileJanitorService janitorService;
  private Cancellable cancellable;

  @Inject
  public StreamHttpService(CConfiguration cConf, DiscoveryService discoveryService,
                           StreamCoordinator streamCoordinator,
                           StreamFileJanitorService janitorService,
                           @Named(Constants.Stream.STREAM_HANDLER) Set<HttpHandler> handlers,
                           @Nullable MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;
    this.streamCoordinator = streamCoordinator;
    this.janitorService = janitorService;

    int workerThreads = cConf.getInt(Constants.Stream.WORKER_THREADS, 10);
    this.httpService = NettyHttpService.builder()
      .addHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Stream.STREAM_HANDLER)))
      .setHost(cConf.get(Constants.Stream.ADDRESS))
      .setWorkerThreadPoolSize(workerThreads)
      .setExecThreadPoolSize(0)
      .setConnectionBacklog(20000)
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.STREAMS));
    httpService.startAndWait();

    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.STREAMS;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    janitorService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    janitorService.stopAndWait();

    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stopAndWait();
      streamCoordinator.close();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }
}
