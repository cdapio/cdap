package com.continuuity.explore.executor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.explore.service.ExploreService;
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
 * Provides various REST endpoints to execute SQL commands via {@link QueryExecutorHttpHandler}.
 * In charge of starting and stopping the {@link com.continuuity.explore.service.ExploreService}.
 */
public class ExploreExecutorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreExecutorService.class);

  private final ExploreService exploreService;
  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public ExploreExecutorService(CConfiguration cConf, DiscoveryService discoveryService,
                                MetricsCollectionService metricsCollectionService,
                                ExploreService exploreService,
                                @Named(Constants.Service.EXPLORE_HTTP_USER_SERVICE) Set<HttpHandler> handlers) {
    this.exploreService = exploreService;
    this.discoveryService = discoveryService;

    int workerThreads = cConf.getInt(Constants.Explore.WORKER_THREADS, 10);
    int execThreads = cConf.getInt(Constants.Explore.EXEC_THREADS, 10);

    this.httpService = NettyHttpService.builder()
        .addHttpHandlers(handlers)
        .setHost(cConf.get(Constants.Explore.SERVER_ADDRESS))
        .setHandlerHooks(ImmutableList.of(
            new MetricsReporterHook(metricsCollectionService, Constants.Service.EXPLORE_HTTP_USER_SERVICE)))
        .setWorkerThreadPoolSize(workerThreads)
        .setExecThreadPoolSize(execThreads)
        .setConnectionBacklog(cConf.getInt(Constants.Explore.BACKLOG_CONNECTIONS, 20000))
        .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.EXPLORE_HTTP_USER_SERVICE));

    LOG.info("Starting {}...", ExploreExecutorService.class.getSimpleName());

    exploreService.startAndWait();

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

    LOG.info("{} started successfully on {}", ExploreExecutorService.class.getSimpleName(),
             httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}...", ExploreExecutorService.class.getSimpleName());

    try {
      // First cancel discoverable so that we don't get any more HTTP requests.
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      try {
        // Then stop HTTP service so that we don't send anymore requests to explore service.
        httpService.stopAndWait();
      } finally {
        // Finally stop explore service
        exploreService.stopAndWait();
      }
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("bindAddress", httpService.getBindAddress())
        .toString();
  }
}
