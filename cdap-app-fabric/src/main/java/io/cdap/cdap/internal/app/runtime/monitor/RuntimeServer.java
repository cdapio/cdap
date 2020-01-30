/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.MetricsReporterHook;
import io.cdap.cdap.gateway.handlers.PingHandler;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;

import java.util.Collections;

/**
 * The runtime server for accepting runtime calls from the program runtime.
 */
public class RuntimeServer extends AbstractIdleService {

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  RuntimeServer(CConfiguration cConf, RuntimeRequestValidator requestValidator,
                DiscoveryService discoveryService, MessagingService messagingService,
                MetricsCollectionService metricsCollectionService) {
    this.httpService = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.RUNTIME_HTTP)
      .setHttpHandlers(new PingHandler(),
                       new RuntimeHandler(requestValidator, new MultiThreadMessagingContext(messagingService)))
      .setExceptionHandler(new HttpExceptionHandler())
      .setHandlerHooks(Collections.singleton(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.RUNTIME_HTTP)))
      .build();
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    httpService.start();
    cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(
      URIScheme.createDiscoverable(Constants.Service.RUNTIME_HTTP, httpService)));
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stop();
  }
}
