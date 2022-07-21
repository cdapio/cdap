/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime.main.transaction;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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

import java.util.Set;

/**
 * Transaction Http service implemented using the common http netty framework.
 * Currently only used for PingHandler.
 */
public class TransactionHttpService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionHttpService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  public TransactionHttpService(CConfiguration cConf, SConfiguration sConf,
                                @Named(Constants.Service.TRANSACTION_HTTP) Set<HttpHandler> handlers,
                                DiscoveryService discoveryService,
                                CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    // netty http server config
    String address = cConf.get(Constants.Transaction.Container.ADDRESS);

    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(Constants.Service.TRANSACTION_HTTP)
      .setHttpHandlers(handlers)
      .setHost(address);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpService = builder.build();
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Transaction HTTP Service...");
    httpService.start();
    // Register the service
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.TRANSACTION_HTTP, httpService)));
    LOG.info("Transaction HTTP started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Transaction HTTP...");

    // Unregister the service
    cancelDiscovery.cancel();
    httpService.stop();
  }
}
