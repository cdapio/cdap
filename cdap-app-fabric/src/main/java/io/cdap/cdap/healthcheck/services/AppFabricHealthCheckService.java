/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.healthcheck.services;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 *
 */
public class AppFabricHealthCheckService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHealthCheckService.class);

  private final DiscoveryService discoveryService;

  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  @Inject
  public AppFabricHealthCheckService(CConfiguration cConf, SConfiguration sConf, DiscoveryService discoveryService,
                                     @Named(Constants.AppFabricHealthCheck.HANDLERS_NAME) Set<HttpHandler> handlers) {
    this.discoveryService = discoveryService;

    NettyHttpService.Builder builder =
      new CommonNettyHttpServiceBuilder(cConf, Constants.Service.APP_FABRIC_HEALTH_CHECK_SERVICE).setHttpHandlers(
          handlers)
        .setHost(cConf.get(Constants.AppFabricHealthCheck.SERVICE_BIND_ADDRESS))
        .setPort(cConf.getInt(Constants.AppFabricHealthCheck.SERVICE_BIND_PORT));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting AppFabricHealthCheckService Service");
    httpService.start();
    InetSocketAddress socketAddress = httpService.getBindAddress();
    LOG.debug("AppFabricHealthCheckService service running at {}", socketAddress);
    cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(
      URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HEALTH_CHECK_SERVICE, httpService)));

  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down SupportBundleInternal Service");
    cancelDiscovery.cancel();
    httpService.stop();
    LOG.debug("SupportBundleInternal HTTP service stopped");
  }
}
