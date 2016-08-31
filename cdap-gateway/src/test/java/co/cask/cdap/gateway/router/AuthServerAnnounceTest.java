/*
 *
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.security.server.GrantAccessToken;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.JsonPrimitive;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.net.DefaultSocketFactory;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.SocketFactory;

public class AuthServerAnnounceTest {
  private static final String HOSTNAME = "127.0.0.1";
  private static final int CONNECTION_IDLE_TIMEOUT_SECS = 2;
  private static final DiscoveryService DISCOVERY_SERVICE = new InMemoryDiscoveryService();
  private static final String ANNOUNCE_ADDRESS = "vip.cask.co:10000";

  @Test
  public void testEmptyAnnounceAddressConfig() throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpRouterService routerService = createRouterService(null);
    routerService.startUp();
    String url = resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v3/apps", routerService);
    HttpGet get = new HttpGet(url);
    HttpResponse response = client.execute(get);
    routerService.shutDown();
    String content = CharStreams.toString(new InputStreamReader(response.getEntity().getContent()));
    Assert.assertEquals(content, routerService.correctAuthServerResponse(null));
  }

  @Test
  public void testAnnounceAddressConfig() throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpRouterService routerService = createRouterService(ANNOUNCE_ADDRESS);
    routerService.startUp();
    String url = resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v3/apps", routerService);
    HttpGet get = new HttpGet(url);
    HttpResponse response = client.execute(get);
    routerService.shutDown();
    String content = CharStreams.toString(new InputStreamReader(response.getEntity().getContent()));
    Assert.assertEquals(content, routerService.correctAuthServerResponse(ANNOUNCE_ADDRESS));
  }


  private HttpRouterService createRouterService(String announceAddress) {
    HttpRouterService routerService = new AuthServerAnnounceTest.HttpRouterService(HOSTNAME, DISCOVERY_SERVICE);
    if (announceAddress != null) {
      routerService.setAuthServerAnnounceAddress(announceAddress);
    }
    return routerService;
  }

  private String resolveURI(String serviceName, String path, HttpRouterService routerService) throws URISyntaxException {
    return getBaseURI(serviceName, routerService).resolve(path).toASCIIString();
  }

  private URI getBaseURI(String serviceName, HttpRouterService routerService) throws URISyntaxException {
    int servicePort = routerService.lookupService(serviceName);
    return new URI(String.format("%s://%s:%d", "http", HOSTNAME, servicePort));
  }

  private static class HttpRouterService extends AbstractIdleService {
    private final String hostname;
    private final DiscoveryService discoveryService;
    private final Map<String, Integer> serviceMap = Maps.newHashMap();
    private CConfiguration cConf = CConfiguration.create();
    private NettyRouter router;

    private HttpRouterService(String hostname, DiscoveryService discoveryService) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
    }

    private String correctAuthServerResponse(String announceAddress) {
      if (announceAddress == null) {
        return "{\"auth_uri\":[]}";
      }
      String protocol;
      if (cConf.getBoolean(Constants.Security.SSL_ENABLED)) {
        protocol = "https";
      } else {
        protocol = "http";
      }

      String url = String.format("%s://%s/%s", protocol, announceAddress,
                          GrantAccessToken.Paths.GET_TOKEN);
      return String.format("{\"auth_uri\":[\"%s\"]}", url);

    }

    private void setAuthServerAnnounceAddress(String announceAddress) {
      cConf.set(Constants.Security.AUTH_SERVER_ANNOUNCE_ADDRESS, announceAddress);
    }

    @Override
    protected void startUp() {
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
      cConf.setInt(Constants.Router.CONNECTION_TIMEOUT_SECS, CONNECTION_IDLE_TIMEOUT_SECS);
      cConf.setBoolean(Constants.Security.ENABLED, true);

      router =
        new NettyRouter(cConf, sConfiguration, InetAddresses.forString(hostname),
                        new RouterServiceLookup((DiscoveryServiceClient) discoveryService,
                                                new RouterPathLookup()),
                        new MissingTokenValidator(), accessTokenTransformer, discoveryServiceClient);
      router.startAndWait();

      for (Map.Entry<Integer, String> entry : router.getServiceLookup().getServiceMap().entrySet()) {
        serviceMap.put(entry.getValue(), entry.getKey());
      }
    }

    @Override
    protected void shutDown() {
      router.stopAndWait();
    }

    public int lookupService(String serviceName) {
      return serviceMap.get(serviceName);
    }
  }

}
