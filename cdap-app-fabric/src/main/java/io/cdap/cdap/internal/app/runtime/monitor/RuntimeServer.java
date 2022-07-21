/*
 * Copyright © 2020-2021 Cask Data, Inc.
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
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.util.concurrent.EventExecutor;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * The runtime server for accepting runtime calls from the program runtime.
 */
public class RuntimeServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeServer.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  RuntimeServer(CConfiguration cConf, SConfiguration sConf, @Named(Constants.Service.RUNTIME) Set<HttpHandler> handlers,
                DiscoveryService discoveryService, CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(Constants.Service.RUNTIME)
      .setHttpHandlers(handlers)
      .setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          if (enableRuntimeIdentity(cConf)) {
            EventExecutor executor = pipeline.context(CommonNettyHttpServiceBuilder.AUTHENTICATOR_NAME).executor();
            pipeline.addBefore(executor, CommonNettyHttpServiceBuilder.AUTHENTICATOR_NAME, "identity-handler",
                               new RuntimeIdentityHandler(cConf));
          }
        }
      })
      .setHost(cConf.get(Constants.RuntimeMonitor.BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.RuntimeMonitor.BIND_PORT));

    if (cConf.getBoolean(Constants.RuntimeMonitor.SSL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpService = builder.build();
    this.discoveryService = discoveryService;
  }

  /**
   * Enable the runtime identity handler if security is enabled AND either internal auth is enabled OR runtime identity
   * backwards compatibility mode is disabled.
   */
  private boolean enableRuntimeIdentity(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.ENABLED, false)
      && (SecurityUtil.isInternalAuthEnabled(cConf)
      || !cConf.getBoolean(Constants.Security.RUNTIME_IDENTITY_COMPATIBILITY_ENABLED));
  }

  @Override
  protected void startUp() throws Exception {
    httpService.start();
    Discoverable discoverable = ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.RUNTIME,
                                                                                      httpService));
    cancelDiscovery = discoveryService.register(discoverable);
    LOG.debug("Runtime server with service name '{}' started on {}:{}", discoverable.getName(),
              discoverable.getSocketAddress().getHostName(), discoverable.getSocketAddress().getPort());
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stop();
  }
}
