/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main.transaction;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.metrics.MetricsReporterHook;
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
  public TransactionHttpService(CConfiguration cConf,
                                @Named(Constants.Service.TRANSACTION_HTTP) Set<HttpHandler> handlers,
                                DiscoveryService discoveryService,
                                MetricsCollectionService metricsCollectionService) {
    // netty http server config
    String address = cConf.get(Constants.Transaction.Container.ADDRESS);

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.TRANSACTION_HTTP);
    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.TRANSACTION_HTTP)));
    builder.setHttpHandlers(handlers);

    builder.setHost(address);

    this.httpService = builder.build();
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Transaction HTTP Service...");
    httpService.start();
    // Register the service
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.TRANSACTION_HTTP, httpService.getBindAddress())));
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
