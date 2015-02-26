/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.executor;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.explore.service.ExploreService;
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
 * Provides various REST endpoints to execute SQL commands via {@link NamespacedQueryExecutorHttpHandler}.
 * In charge of starting and stopping the {@link co.cask.cdap.explore.service.ExploreService}.
 */
public class ExploreExecutorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreExecutorService.class);

  private final ExploreService exploreService;
  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final boolean startOnDemand;
  private Cancellable cancellable;

  @Inject
  public ExploreExecutorService(CConfiguration cConf, DiscoveryService discoveryService,
                                MetricsCollectionService metricsCollectionService,
                                ExploreService exploreService,
                                @Named(Constants.Service.EXPLORE_HTTP_USER_SERVICE) Set<HttpHandler> handlers) {
    this.exploreService = exploreService;
    this.discoveryService = discoveryService;
    this.startOnDemand = cConf.getBoolean(Constants.Explore.START_ON_DEMAND);

    int workerThreads = cConf.getInt(Constants.Explore.WORKER_THREADS, 10);
    int execThreads = cConf.getInt(Constants.Explore.EXEC_THREADS, 10);

    this.httpService = new CommonNettyHttpServiceBuilder(cConf)
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

    if (!startOnDemand) {
      exploreService.startAndWait();
    }

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
