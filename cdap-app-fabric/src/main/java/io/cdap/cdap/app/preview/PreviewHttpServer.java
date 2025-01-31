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

package io.cdap.cdap.app.preview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.util.Set;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP Server for preview.
 */
public class PreviewHttpServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServer.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final PreviewManager previewManager;
  private Cancellable cancelHttpService;

  @Inject
  PreviewHttpServer(CConfiguration cConf, SConfiguration sConf,
      DiscoveryService discoveryService, Set<HttpHandler> httpHandlers,
      PreviewManager previewManager, CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    this.discoveryService = discoveryService;
    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(
            Constants.Service.PREVIEW_HTTP)
        .setHost(cConf.get(Constants.Preview.ADDRESS))
        .setPort(cConf.getInt(Constants.Preview.PORT))
        .setHttpHandlers(httpHandlers)
        .setConnectionBacklog(cConf.getInt(Constants.Preview.BACKLOG_CONNECTIONS))
        .setExecThreadPoolSize(cConf.getInt(Constants.Preview.EXEC_THREADS))
        .setBossThreadPoolSize(cConf.getInt(Constants.Preview.BOSS_THREADS))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.Preview.WORKER_THREADS))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        });

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpService = builder.build();
    this.previewManager = previewManager;
  }

  /**
   * Returns the {@link PreviewManager} used by this server for serving HTTP requests.
   */
  @VisibleForTesting
  public PreviewManager getPreviewManager() {
    return previewManager;
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(
        new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
            Constants.Logging.COMPONENT_NAME,
            Constants.Service.PREVIEW_HTTP));
    if (previewManager instanceof Service) {
      ((Service) previewManager).startAndWait();
    }

    httpService.start();

    cancelHttpService = discoveryService.register(
        ResolvingDiscoverable.of(
            URIScheme.createDiscoverable(Constants.Service.PREVIEW_HTTP, httpService)));
    LOG.info("Preview HTTP server started on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      cancelHttpService.cancel();
      if (previewManager instanceof Service) {
        ((Service) previewManager).stopAndWait();
      }
    } finally {
      httpService.stop();
    }
    LOG.info("Preview HTTP server stopped");
  }
}
