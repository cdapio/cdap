package com.continuuity.logging.service;

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

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * LogSaver Service, Currently only used for PingHandler, so this service can be discovered during reactor-startup
 */
public class LogSaverStatusService extends AbstractIdleService {
  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public LogSaverStatusService(CConfiguration cConf, DiscoveryService discoveryService,
                               @Named(Constants.LogSaver.LOG_SAVER_STATUS_HANDLER) Set<HttpHandler> handlers,
                               MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;
    this.httpService = NettyHttpService.builder()
      .addHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.LogSaver.LOG_SAVER_STATUS_HANDLER)))
      .setHost(cConf.get(Constants.LogSaver.ADDRESS))
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.LOGSAVER));
    httpService.startAndWait();

    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.LOGSAVER;
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
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }
}
