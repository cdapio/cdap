/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

package io.cdap.cdap.logging.service;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
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
  LogQueryService(CConfiguration cConf, SConfiguration sConf,
                  @Named(Constants.Service.LOG_QUERY) Set<HttpHandler> handlers,
                  DiscoveryService discoveryService, CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    this.discoveryService = discoveryService;
    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(Constants.Service.LOG_QUERY)
      .setHttpHandlers(handlers)
      .setHost(cConf.get(Constants.LogQuery.ADDRESS))
      .setPort(cConf.getInt(Constants.LogQuery.PORT));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpServer = builder.build();

  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.LOG_QUERY));
    httpServer.start();
    cancellable = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.LOG_QUERY, httpServer)));

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
