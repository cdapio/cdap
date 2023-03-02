/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.support.services;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import java.net.InetSocketAddress;
import java.util.Set;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple service for managing the lifecycle of the http service that hosts the support bundle
 * REST api.
 */
public class SupportBundleInternalService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleInternalService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  @Inject
  SupportBundleInternalService(CConfiguration cConf, SConfiguration sConf,
      DiscoveryService discoveryService,
      @Named(Constants.SupportBundle.HANDLERS_NAME) Set<HttpHandler> handlers,
      CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    this.discoveryService = discoveryService;

    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(
            Constants.Service.SUPPORT_BUNDLE_SERVICE)
        .setHttpHandlers(handlers)
        .setExceptionHandler(new HttpExceptionHandler())
        .setHost(cConf.get(Constants.SupportBundle.SERVICE_BIND_ADDRESS))
        .setPort(cConf.getInt(Constants.SupportBundle.SERVICE_BIND_PORT))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.SupportBundle.SERVICE_WORKER_THREADS))
        .setExecThreadPoolSize(cConf.getInt(Constants.SupportBundle.SERVICE_EXEC_THREADS))
        .setConnectionBacklog(10000);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting SupportBundleInternal Service");
    httpService.start();
    InetSocketAddress socketAddress = httpService.getBindAddress();
    LOG.debug("SupportBundleInternal service running at {}", socketAddress);
    cancelDiscovery = discoveryService.register(
        ResolvingDiscoverable.of(
            URIScheme.createDiscoverable(Constants.Service.SUPPORT_BUNDLE_SERVICE, httpService)));

  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down SupportBundleInternal Service");
    cancelDiscovery.cancel();
    httpService.stop();
    LOG.debug("SupportBundleInternal HTTP service stopped");
  }
}
