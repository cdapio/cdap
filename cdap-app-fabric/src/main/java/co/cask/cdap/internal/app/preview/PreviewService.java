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

package co.cask.cdap.internal.app.preview;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
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
import javax.annotation.Nullable;

/**
 * Preview service using the common netty http framework.
 */
public class PreviewService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  public PreviewService(CConfiguration cConf, @Named(Constants.Service.PREVIEW_HTTP) Set<HttpHandler> handlers,
                        DiscoveryService discoveryService,
                        @Nullable MetricsCollectionService metricsCollectionService) {
    String address = cConf.get(Constants.Preview.ADDRESS);
    NettyHttpService.Builder httpServiceBuilder = new CommonNettyHttpServiceBuilder(cConf,
                                                                                    Constants.Service.PREVIEW_HTTP);
    httpServiceBuilder.setHandlerHooks(
      ImmutableList.of(new MetricsReporterHook(metricsCollectionService, Constants.Service.PREVIEW_HTTP)));
    httpServiceBuilder.setHttpHandlers(handlers);
    httpServiceBuilder.setHost(address);

    this.httpService = httpServiceBuilder.build();
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREVIEW_HTTP));

    LOG.info("Starting Preview Service...");
    httpService.start();
    LOG.info("Started Preview HTTP Service...");
    // Register the service
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.METRICS, httpService.getBindAddress())));
    LOG.info("Preview Service started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Preview Service...");

    // Unregister the service
    cancelDiscovery.cancel();
    httpService.stop();
  }
}
