/*
 * Copyright © 2014-2021 Cask Data, Inc.
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
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.ExternalAuthenticationModule;
import org.apache.commons.net.DefaultSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.InetSocketAddress;
import javax.net.SocketFactory;

/**
 * Tests Netty Router running on HTTP.
 */
public class NettyRouterHttpTest extends NettyRouterTestBase {

  @Override
  protected RouterService createRouterService(String hostname, DiscoveryService discoveryService) {
    return new HttpRouterService(hostname, discoveryService);
  }

  @Override
  protected String getProtocol() {
    return "http";
  }

  @Override
  protected DefaultHttpClient getHTTPClient() {
    return new DefaultHttpClient();
  }

  @Override
  protected SocketFactory getSocketFactory() {
    return new DefaultSocketFactory();
  }

  private static class HttpRouterService extends RouterService {
    private final String hostname;
    private final DiscoveryService discoveryService;

    private NettyRouter router;

    private HttpRouterService(String hostname, DiscoveryService discoveryService) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
    }

    @Override
    protected void startUp() {
      CConfiguration cConf = CConfiguration.create();
      SConfiguration sConfiguration = SConfiguration.create();
      Injector injector = Guice.createInjector(new CoreSecurityRuntimeModule().getInMemoryModules(),
                                               new ExternalAuthenticationModule(),
                                               new InMemoryDiscoveryModule(),
                                               new AppFabricTestModule(cConf));
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      UserIdentityExtractor userIdentityExtractor = injector.getInstance(UserIdentityExtractor.class);
      cConf.set(Constants.Router.ADDRESS, hostname);
      cConf.setInt(Constants.Router.ROUTER_PORT, 0);
      cConf.setInt(Constants.Router.CONNECTION_TIMEOUT_SECS, CONNECTION_IDLE_TIMEOUT_SECS);
      router =
        new NettyRouter(cConf, sConfiguration, InetAddresses.forString(hostname),
                        new RouterServiceLookup(cConf, (DiscoveryServiceClient) discoveryService,
                                                new RouterPathLookup()),
                        new SuccessTokenValidator(), userIdentityExtractor, discoveryServiceClient,
                        new NoOpMetricsCollectionService());
      router.startAndWait();
    }

    @Override
    protected void shutDown() {
      router.stopAndWait();
    }

    public InetSocketAddress getRouterAddress() {
      return router.getBoundAddress().orElseThrow(IllegalStateException::new);
    }
  }
}
