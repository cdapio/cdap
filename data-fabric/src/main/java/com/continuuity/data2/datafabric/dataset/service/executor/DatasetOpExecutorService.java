package com.continuuity.data2.datafabric.dataset.service.executor;

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

/**
 * Provides various REST endpoints to execute user code via {@link DatasetAdminOpHTTPHandler}.
 */
public class DatasetOpExecutorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetOpExecutorService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public DatasetOpExecutorService(CConfiguration cConf, DiscoveryService discoveryService,
                                  MetricsCollectionService metricsCollectionService,
                                  @Named(Constants.Service.DATASET_EXECUTOR) Set<HttpHandler> handlers) {

    this.discoveryService = discoveryService;

    int workerThreads = cConf.getInt(Constants.Dataset.Executor.WORKER_THREADS, 10);
    int execThreads = cConf.getInt(Constants.Dataset.Executor.EXEC_THREADS, 10);

    this.httpService = NettyHttpService.builder()
      .addHttpHandlers(handlers)
      .setHost(cConf.get(Constants.Dataset.Executor.ADDRESS))
      .setHandlerHooks(ImmutableList.of(
        new MetricsReporterHook(metricsCollectionService, Constants.Service.DATASET_EXECUTOR)))
      .setWorkerThreadPoolSize(workerThreads)
      .setExecThreadPoolSize(execThreads)
      .setConnectionBacklog(20000)
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.DATASET_EXECUTOR));
    LOG.info("Starting DatasetOpExecutorService...");

    httpService.startAndWait();
    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.DATASET_EXECUTOR;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    LOG.info("DatasetOpExecutorService started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping DatasetOpExecutorService...");

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
