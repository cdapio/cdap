package com.continuuity.metrics.query;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
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
import javax.annotation.Nullable;

/**
 * Metrics implemented using the common http netty framework.
 */
public class MetricsQueryService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsQueryService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  public MetricsQueryService(CConfiguration cConf, @Named(Constants.Service.METRICS) Set<HttpHandler> handlers,
                             DiscoveryService discoveryService,
                             @Nullable MetricsCollectionService metricsCollectionService) {
    // netty http server config
    String address = cConf.get(Constants.Metrics.ADDRESS);
    int backlogcnxs = cConf.getInt(Constants.Metrics.BACKLOG_CONNECTIONS, 20000);
    int execthreads = cConf.getInt(Constants.Metrics.EXEC_THREADS, 20);
    int bossthreads = cConf.getInt(Constants.Metrics.BOSS_THREADS, 1);
    int workerthreads = cConf.getInt(Constants.Metrics.WORKER_THREADS, 10);

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers);
    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.METRICS)));

    builder.setHost(address);

    builder.setConnectionBacklog(backlogcnxs);
    builder.setExecThreadPoolSize(execthreads);
    builder.setBossThreadPoolSize(bossthreads);
    builder.setWorkerThreadPoolSize(workerthreads);

    this.httpService = builder.build();
    this.discoveryService = discoveryService;

    LOG.info("Configuring MetricsService " +
               ", address: " + address +
               ", backlog connections: " + backlogcnxs +
               ", execthreads: " + execthreads +
               ", bossthreads: " + bossthreads +
               ", workerthreads: " + workerthreads);
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.METRICS));

    LOG.info("Starting Metrics Service...");
    httpService.startAndWait();
    LOG.info("Started Metrics HTTP Service...");
    // Register the service
    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.METRICS;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    LOG.info("Metrics Service started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Metrics Service...");

    // Unregister the service
    cancelDiscovery.cancel();
    httpService.stopAndWait();
  }
}
