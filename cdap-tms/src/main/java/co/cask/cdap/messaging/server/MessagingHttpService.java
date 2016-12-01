/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.server;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * The http server for supporting messaging system REST API.
 */
public class MessagingHttpService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingHttpService.class);

  private final CConfiguration cConf;
  private final DiscoveryService discoveryService;
  private final MetricsCollectionService metricsCollectionService;
  private final Set<HttpHandler> handlers;
  private NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  @Inject
  public MessagingHttpService(CConfiguration cConf, DiscoveryService discoveryService,
                              MetricsCollectionService metricsCollectionService,
                              MessagingService messagingService,
                              @Named(Constants.MessagingSystem.HANDLER_BINDING_NAME) Set<HttpHandler> handlers) {
    this.cConf = cConf;
    this.discoveryService = discoveryService;
    this.metricsCollectionService = metricsCollectionService;
    this.handlers = handlers;
  }

  @Override
  protected void startUp() throws Exception {
    httpService = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.MESSAGING_SERVICE)
      .setHost(cConf.get(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS))
      .setHandlerHooks(ImmutableList.of(
        new MetricsReporterHook(metricsCollectionService, Constants.Service.MESSAGING_SERVICE)))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_WORKER_THREADS))
      .setExecThreadPoolSize(cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_EXECUTOR_THREADS))
      .setHttpChunkLimit(cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_MAX_REQUEST_SIZE_MB))
      .setExceptionHandler(new HttpExceptionHandler() {
        @Override
        public void handle(Throwable t, HttpRequest request, HttpResponder responder) {
          // TODO: CDAP-7688. Override the handling to return 400 on IllegalArgumentException
          if (t instanceof IllegalArgumentException) {
            logWithTrace(request, t);
            responder.sendString(HttpResponseStatus.BAD_REQUEST, t.getMessage());
          } else {
            super.handle(t, request, responder);
          }
        }

        private void logWithTrace(HttpRequest request, Throwable t) {
          LOG.trace("Error in handling request={} {} for user={}:", request.getMethod().getName(), request.getUri(),
                    Objects.firstNonNull(SecurityRequestContext.getUserId(), "<null>"), t);
        }
      })
      .addHttpHandlers(handlers)
      .build();
    httpService.startAndWait();

    cancelDiscovery = discoveryService.register(new Discoverable(Constants.Service.MESSAGING_SERVICE,
                                                                 httpService.getBindAddress()));
    LOG.info("Messaging HTTP server started on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      cancelDiscovery.cancel();
    } finally {
      httpService.stopAndWait();
    }
    LOG.info("Messaging HTTP server stopped");
  }
}
