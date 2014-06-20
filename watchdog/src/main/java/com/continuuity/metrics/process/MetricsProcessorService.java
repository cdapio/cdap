package com.continuuity.metrics.process;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
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
import javax.annotation.Nullable;

/**
 * MetricsProcessorService with PingHandler used for discovery during reactor-services startup
 */
public class MetricsProcessorService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorService.class);
  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public MetricsProcessorService(CConfiguration cConf, DiscoveryService discoveryService,
                         @Named(Constants.MetricsProcessor.METRICS_PROCESSOR_HANDLER) Set<HttpHandler> handlers,
                         @Nullable MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;
    int workerThreads =  10;
    this.httpService = NettyHttpService.builder()
      .addHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.MetricsProcessor.METRICS_PROCESSOR_HANDLER)))
      .setHost(cConf.get(Constants.MetricsProcessor.ADDRESS))
      .setWorkerThreadPoolSize(workerThreads)
      .setExecThreadPoolSize(0)
      .setConnectionBacklog(20000)
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.METRICS_PROCESSOR));
    httpService.startAndWait();
    LOG.info("MetricsProcessor Service started");

    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.METRICS_PROCESSOR;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stopAndWait();
      LOG.info("Metrics Processor Service Stopped");
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }
}
