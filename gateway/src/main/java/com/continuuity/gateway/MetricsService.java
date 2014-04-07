package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.gateway.hooks.MetricsReporterHook;
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

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Metrics implemented using the common http netty framework.
 */
public class MetricsService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  public MetricsService(CConfiguration cConf,
                 @Named(Constants.Metrics.ADDRESS) InetAddress hostname,
                 @Named("metrics") Set<HttpHandler> handlers, DiscoveryService discoveryService,
                 @Nullable MetricsCollectionService metricsCollectionService) {

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers);
    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService)));

    builder.setHost(hostname.getCanonicalHostName());
    builder.setPort(cConf.getInt(Constants.Metrics.PORT, Constants.Metrics.DEFAULT_PORT));

    //TODO: Change to Metrics Constants?
    builder.setConnectionBacklog(cConf.getInt(Constants.Gateway.BACKLOG_CONNECTIONS,
                                              Constants.Gateway.DEFAULT_BACKLOG));
    builder.setExecThreadPoolSize(cConf.getInt(Constants.Gateway.EXEC_THREADS,
                                               Constants.Gateway.DEFAULT_EXEC_THREADS));
    builder.setBossThreadPoolSize(cConf.getInt(Constants.Gateway.BOSS_THREADS,
                                               Constants.Gateway.DEFAULT_BOSS_THREADS));
    builder.setWorkerThreadPoolSize(cConf.getInt(Constants.Gateway.WORKER_THREADS,
                                                 Constants.Gateway.DEFAULT_WORKER_THREADS));

    this.httpService = builder.build();
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
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
    // Wait for a few seconds for requests to stop
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting...", e);
    }

    httpService.stopAndWait();
  }

  public InetSocketAddress getBindAddress() {
    return httpService.getBindAddress();
  }
}
