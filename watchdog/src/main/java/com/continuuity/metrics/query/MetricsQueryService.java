/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Entry point for metrics query server.
 */
public final class MetricsQueryService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsQueryService.class);

  private static final int DEFAULT_THREAD_POOL_SIZE = 30;
  private static final int DEFAULT_KEEP_ALIVE_SECONDS = 30;

  private final InetAddress hostname;
  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;

  private Cancellable cancelDiscovery;

  @Inject
  public MetricsQueryService(CConfiguration cConf,
                             @Named("config.hostname") InetAddress hostname,
                             DiscoveryService discoveryService,
                             Set<HttpHandler> handlers) {
    this.httpService = new NettyHttpService(cConf.getInt(MetricsConstants.ConfigKeys.SERVER_PORT, 0),
                                            cConf.getInt(MetricsConstants.ConfigKeys.THREAD_POOL_SIZE,
                                                         DEFAULT_THREAD_POOL_SIZE),
                                            cConf.getInt(MetricsConstants.ConfigKeys.KEEP_ALIVE_SECONDS,
                                                         DEFAULT_KEEP_ALIVE_SECONDS),
                                            handlers);
    this.hostname = hostname;
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    httpService.startAndWait();
    final InetSocketAddress address = new InetSocketAddress(hostname, httpService.getServicePort());
    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.SERVICE_METRICS;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return address;
      }
    });

    LOG.info("Metrics query service started at address {}", address);
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stopAndWait();

    LOG.info("Metrics query service stopped.");
  }
}
