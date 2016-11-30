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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.proto.Id;
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
  public MetricsProcessorStatusService (CConfiguration cConf, DiscoveryService discoveryService,
                                        @Named(Constants.MetricsProcessor.METRICS_PROCESSOR_STATUS_HANDLER)
                                        Set<HttpHandler> handlers,
                                        MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;
    this.httpService = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.METRICS_PROCESSOR)
      .addHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                        Constants.MetricsProcessor.METRICS_PROCESSOR_STATUS_HANDLER)))
      .setHost(cConf.get(Constants.MetricsProcessor.ADDRESS))
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.METRICS_PROCESSOR));
    LOG.info("Starting MetricsProcessor Status Service...");

    httpService.startAndWait();

    cancellable = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.METRICS_PROCESSOR, httpService.getBindAddress())));
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
      httpService.stopAndWait();
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
