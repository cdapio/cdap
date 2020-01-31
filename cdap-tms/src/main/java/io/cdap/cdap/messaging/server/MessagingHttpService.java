/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

package io.cdap.cdap.messaging.server;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.MetricsReporterHook;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * The http server for supporting messaging system REST API.
 */
public class MessagingHttpService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingHttpService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  @Inject
  public MessagingHttpService(CConfiguration cConf, SConfiguration sConf, DiscoveryService discoveryService,
                              MetricsCollectionService metricsCollectionService,
                              @Named(Constants.MessagingSystem.HANDLER_BINDING_NAME) Set<HttpHandler> handlers) {
    this.discoveryService = discoveryService;

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.MESSAGING_SERVICE)
      .setHost(cConf.get(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_BIND_PORT))
      .setHandlerHooks(Collections.singleton(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.MESSAGING_SERVICE)))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_WORKER_THREADS))
      .setExecThreadPoolSize(cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_EXECUTOR_THREADS))
      .setHttpChunkLimit(cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_MAX_REQUEST_SIZE_MB) * 1024 * 1024)
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
          LOG.trace("Error in handling request={} {} for user={}:", request.method().name(), request.uri(),
                    Objects.firstNonNull(SecurityRequestContext.getUserId(), "<null>"), t);
        }
      })
      .setHttpHandlers(handlers);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    httpService.start();
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.MESSAGING_SERVICE, httpService)));
    LOG.info("Messaging HTTP server started on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      cancelDiscovery.cancel();
    } finally {
      httpService.stop();
    }
    LOG.info("Messaging HTTP server stopped");
  }
}
