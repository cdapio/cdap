/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.route.store.RouteStore;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.guice.SecurityModules;
import com.google.common.net.InetAddresses;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.rules.ExternalResource;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

class RouterResource extends ExternalResource {
  private final String hostname;
  private final DiscoveryService discoveryService;
  private final Map<String, String> additionalConfig;

  private NettyRouter router;

  RouterResource(String hostname, DiscoveryService discoveryService) {
    this(hostname, discoveryService, new HashMap<>());
  }

  RouterResource(String hostname, DiscoveryService discoveryService, Map<String, String> additionalConfig) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.additionalConfig = additionalConfig;
  }

  @Override
  protected void before() {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new SecurityModules().getInMemoryModules(),
                                             new DiscoveryRuntimeModule().getInMemoryModules(),
                                             new AppFabricTestModule(cConf));
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    AccessTokenTransformer accessTokenTransformer = new MockAccessTokenTransfomer();
    RouteStore routeStore = injector.getInstance(RouteStore.class);
    SConfiguration sConf = injector.getInstance(SConfiguration.class);
    cConf.set(Constants.Router.ADDRESS, hostname);
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);
    for (Map.Entry<String, String> entry : additionalConfig.entrySet()) {
      cConf.set(entry.getKey(), entry.getValue());
    }
    router =
      new NettyRouter(cConf, sConf, InetAddresses.forString(hostname),
                      new RouterServiceLookup(cConf, (DiscoveryServiceClient) discoveryService,
                                              new RouterPathLookup(), routeStore),
                      new MockTokenValidator("failme"), accessTokenTransformer, discoveryServiceClient);
    router.startAndWait();
  }

  @Override
  protected void after() {
    router.stopAndWait();
  }

  InetSocketAddress getRouterAddress() {
    return router.getBoundAddress().orElseThrow(IllegalStateException::new);
  }
}
