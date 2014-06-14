package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.metrics.MetricsCollectionService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 *
 */
public class ExploreHttpService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreHttpService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public ExploreHttpService(CConfiguration cConf, DiscoveryService discoveryService,
                            MetricsCollectionService metricsCollectionService,
                            @Named(Constants.Service.EXPLORE_HTTP_USER_SERVICE) Set<HttpHandler> handlers) {

    this.discoveryService = discoveryService;

    int workerThreads = cConf.getInt(Constants.Explore.WORKER_THREADS, 10);
    int execThreads = cConf.getInt(Constants.Explore.EXEC_THREADS, 10);

    this.httpService = NettyHttpService.builder()
        .addHttpHandlers(handlers)
        .setHost(cConf.get(Constants.Explore.SERVER_ADDRESS))
        // TODO do we need these handler hooks?
        .setHandlerHooks(ImmutableList.of(
            new MetricsReporterHook(metricsCollectionService, Constants.Service.EXPLORE_HTTP_USER_SERVICE)))
        .setWorkerThreadPoolSize(workerThreads)
        .setExecThreadPoolSize(execThreads)
        .setConnectionBacklog(20000)
        .build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}...", ExploreHttpService.class.getSimpleName());

    httpService.startAndWait();
    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    LOG.info("{} started successfully on {}", ExploreHttpService.class.getSimpleName(), httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}...", ExploreHttpService.class.getSimpleName());

    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stopAndWait();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("bindAddress", httpService.getBindAddress())
        .toString();
  }

  public NettyHttpService getHttpService() {
    return httpService;
  }

}
