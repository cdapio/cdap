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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.RuntimeMonitor;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.util.Collections;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The runtime server for accepting runtime calls from the program runtime.
 */
public class InternalRouterService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(
      InternalRouterService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  InternalRouterService(CConfiguration cConf, SConfiguration sConf,
      DiscoveryService discoveryService,
      InternalRouterHandler handler,
      CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(
            Constants.Service.RUNTIME)
        .setHttpHandlers(Collections.singleton(handler))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .setHost(cConf.get(Constants.RuntimeMonitor.BIND_ADDRESS))
        .setPort(cConf.getInt(RuntimeMonitor.BIND_PORT_WOKKER, 11115));

    if (cConf.getBoolean(Constants.RuntimeMonitor.SSL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpService = builder.build();
    this.discoveryService = discoveryService;
  }


  @Override
  protected void startUp() throws Exception {
    httpService.start();
    Discoverable discoverable = ResolvingDiscoverable.of(
        URIScheme.createDiscoverable(Service.INTERNAL_ROUTER,
            httpService));
    cancelDiscovery = discoveryService.register(discoverable);
    LOG.debug("Internal router server with service name '{}' started on {}:{}", discoverable.getName(),
        discoverable.getSocketAddress().getHostName(), discoverable.getSocketAddress().getPort());
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stop();
  }
}
