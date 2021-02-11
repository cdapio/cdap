/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import com.google.common.net.InetAddresses;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import io.cdap.cdap.security.guice.SecurityModules;
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
                                             new InMemoryDiscoveryModule(),
                                             new AppFabricTestModule(cConf));
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    TokenValidator mockValidator = new MockTokenValidator("failme");
    UserIdentityExtractor extractor = new MockAccessTokenIdentityExtractor(mockValidator);
    SConfiguration sConf = injector.getInstance(SConfiguration.class);
    cConf.set(Constants.Router.ADDRESS, hostname);
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);
    for (Map.Entry<String, String> entry : additionalConfig.entrySet()) {
      cConf.set(entry.getKey(), entry.getValue());
    }
    router =
      new NettyRouter(cConf, sConf, InetAddresses.forString(hostname),
                      new RouterServiceLookup(cConf, (DiscoveryServiceClient) discoveryService, new RouterPathLookup()),
                      mockValidator, extractor, discoveryServiceClient);
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
