/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

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
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Service to manage metadata in CDAP. This service serves the HTTP endpoints defined in {@link MetadataHttpHandler}.
 */
public class MetadataService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataService.class);

  private final DiscoveryService discoveryService;

  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  @Inject
  MetadataService(CConfiguration cConf, SConfiguration sConf,
                  DiscoveryService discoveryService, CommonNettyHttpServiceFactory commonNettyHttpServiceFactory,
                  @Named(Constants.Metadata.HANDLERS_NAME) Set<HttpHandler> handlers) {
    this.discoveryService = discoveryService;

    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(Constants.Service.METADATA_SERVICE)
      .setHttpHandlers(handlers)
      .setExceptionHandler(new HttpExceptionHandler())
      .setHost(cConf.get(Constants.Metadata.SERVICE_BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.Metadata.SERVICE_BIND_PORT))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.Metadata.SERVICE_WORKER_THREADS))
      .setExecThreadPoolSize(cConf.getInt(Constants.Metadata.SERVICE_EXEC_THREADS))
      .setConnectionBacklog(20000);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    httpService = builder.build();

  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Metadata Service");
    httpService.start();
    InetSocketAddress socketAddress = httpService.getBindAddress();
    LOG.info("Metadata service running at {}", socketAddress);
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.METADATA_SERVICE, httpService)));

  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down Metadata Service");
    cancelDiscovery.cancel();
    httpService.stop();
    LOG.info("Metadata HTTP service stopped");
  }
}
