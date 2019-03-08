/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.service;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.util.Set;

/**
 * Provides the HTTP server for querying logs.
 */
public class LogQueryService extends AbstractIdleService {

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpServer;
  private Cancellable cancellable;

  @Inject
  LogQueryService(CConfiguration cConf, @Named(Constants.Service.LOG_QUERY) Set<HttpHandler> handlers,
                  DiscoveryService discoveryService, MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;
    this.httpServer = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.LOG_QUERY)
      .setHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Service.LOG_QUERY)))
      .setHost(cConf.get(Constants.LogQuery.ADDRESS))
      .setPort(cConf.getInt(Constants.LogQuery.PORT))
      .build();

  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.LOG_QUERY));
    httpServer.start();
    cancellable = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.LOG_QUERY, httpServer.getBindAddress())));

  }

  @Override
  protected void shutDown() throws Exception {
    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpServer.stop();
    }
  }
}
