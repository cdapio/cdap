/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import com.google.common.base.Objects;
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

import java.util.Collections;
import java.util.Set;

/**
 * MetricsProcessorStatusService with PingHandler used for discovery during CDAP-services startup.
 */
public class MetricsProcessorStatusService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorStatusService.class);
  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public MetricsProcessorStatusService(CConfiguration cConf, SConfiguration sConf, DiscoveryService discoveryService,
                                       @Named(Constants.MetricsProcessor.METRICS_PROCESSOR_STATUS_HANDLER)
                                       Set<HttpHandler> handlers,
                                       MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.METRICS_PROCESSOR)
      .setHttpHandlers(handlers)
      .setHandlerHooks(Collections.singleton(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.METRICS_PROCESSOR)))
      .setHost(cConf.get(Constants.MetricsProcessor.BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.MetricsProcessor.BIND_PORT));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.METRICS_PROCESSOR));
    LOG.info("Starting MetricsProcessor Status Service...");

    httpService.start();

    cancellable = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.METRICS_PROCESSOR, httpService)));
    LOG.info("Started MetricsProcessor Status Service.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping MetricsProcessor Status Service...");
    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stop();
      LOG.info("MetricsProcessor Status Service Stopped");
    }
    LOG.info("Stopped MetricsProcessor Status Service.");
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }
}
