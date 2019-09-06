/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.query;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.metrics.MetricsReporterHook;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public MetricsQueryService(CConfiguration cConf, SConfiguration sConf,
                             @Named(Constants.Service.METRICS) Set<HttpHandler> handlers,
                             DiscoveryService discoveryService,
                             @Nullable MetricsCollectionService metricsCollectionService) {
    // netty http server config
    String address = cConf.get(Constants.Metrics.ADDRESS);
    int backlogcnxs = cConf.getInt(Constants.Metrics.BACKLOG_CONNECTIONS, 20000);
    int execthreads = cConf.getInt(Constants.Metrics.EXEC_THREADS, 20);
    int bossthreads = cConf.getInt(Constants.Metrics.BOSS_THREADS, 1);
    int workerthreads = cConf.getInt(Constants.Metrics.WORKER_THREADS, 10);

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.METRICS)
      .setHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService, Constants.Service.METRICS)))
      .setHost(address)
      .setPort(cConf.getInt(Constants.Metrics.PORT))
      .setConnectionBacklog(backlogcnxs)
      .setExecThreadPoolSize(execthreads)
      .setBossThreadPoolSize(bossthreads)
      .setWorkerThreadPoolSize(workerthreads);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

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
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.METRICS));

    LOG.info("Starting Metrics Service...");
    httpService.start();
    LOG.info("Started Metrics HTTP Service...");
    // Register the service
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.METRICS, httpService)));
    LOG.info("Metrics Service started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Metrics Service...");

    // Unregister the service
    cancelDiscovery.cancel();
    httpService.stop();
  }
}
