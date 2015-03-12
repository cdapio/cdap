/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
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

    this.httpService = new CommonNettyHttpServiceBuilder(cConf)
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
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.SYSTEM_NAMESPACE,
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
