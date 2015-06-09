/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.guice.SecurityModules;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Map;

/**
 * Tests Netty Router running on HTTP.
 */
public class NettyRouterHttpTest extends NettyRouterTestBase {

  @Override
  protected int lookupService(String serviceName) {
    return super.lookupService(serviceName);
  }

  @Override
  protected RouterService createRouterService() {
    return new HttpRouterService(HOSTNAME, DISCOVERY_SERVICE);
  }

  @Override
  protected String getProtocol() {
    return "http";
  }

  @Override
  protected DefaultHttpClient getHTTPClient() throws Exception {
    return new DefaultHttpClient();
  }

  private static class HttpRouterService extends RouterService {
    private final String hostname;
    private final DiscoveryService discoveryService;
    private final Map<String, Integer> serviceMap = Maps.newHashMap();

    private NettyRouter router;

    private HttpRouterService(String hostname, DiscoveryService discoveryService) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
    }

    @Override
    protected void startUp() {
      CConfiguration cConf = CConfiguration.create();
      SConfiguration sConfiguration = SConfiguration.create();
      Injector injector = Guice.createInjector(new ConfigModule(cConf), new IOModule(),
                                               new SecurityModules().getInMemoryModules(),
                                               new DiscoveryRuntimeModule().getInMemoryModules());
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      AccessTokenTransformer accessTokenTransformer = injector.getInstance(AccessTokenTransformer.class);
      cConf.set(Constants.Router.ADDRESS, hostname);
      cConf.setInt(Constants.Router.ROUTER_PORT, 0);
      cConf.setBoolean(Constants.Router.WEBAPP_ENABLED, true);
      cConf.setInt(Constants.Router.WEBAPP_PORT, 0);
      router =
        new NettyRouter(cConf, sConfiguration, InetAddresses.forString(hostname),
                        new RouterServiceLookup((DiscoveryServiceClient) discoveryService,
                                                new RouterPathLookup()),
                        new SuccessTokenValidator(), accessTokenTransformer, discoveryServiceClient);
      router.startAndWait();

      for (Map.Entry<Integer, String> entry : router.getServiceLookup().getServiceMap().entrySet()) {
        serviceMap.put(entry.getValue(), entry.getKey());
      }
    }

    @Override
    protected void shutDown() {
      router.stopAndWait();
    }

    @Override
    public int lookupService(String serviceName) {
      return serviceMap.get(serviceName);
    }
  }

}
