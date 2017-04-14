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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.security.server.LDAPLoginModule;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLSocket;

/**
 *
 */
public class AppFabricServerTest {

  @Test
  public void startStopServer() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricServer server = injector.getInstance(AppFabricServer.class);
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Service.State state = server.startAndWait();
    Assert.assertTrue(state == Service.State.RUNNING);

    final EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
      discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP));
    Assert.assertNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS));

    state = server.stopAndWait();
    Assert.assertTrue(state == Service.State.TERMINATED);

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return endpointStrategy.pick() == null;
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testSSL() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, true);
    cConf.setInt(Constants.AppFabric.SERVER_SSL_PORT, 20443);
    SConfiguration sConf = SConfiguration.create();
    final Injector injector = AppFabricTestHelper.getInjector(cConf, sConf, new AbstractModule() {
      @Override
      protected void configure() {
        // no overrides
      }
    });

    final DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    AppFabricServer appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    Assert.assertTrue(appFabricServer.isRunning());

    Supplier<EndpointStrategy> endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP));
      }
    });
    Discoverable discoverable = endpointStrategySupplier.get().pick(3, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    Assert.assertArrayEquals(Constants.Security.SSL_URI_SCHEME.getBytes(), discoverable.getPayload());
    InetSocketAddress addr = discoverable.getSocketAddress();
    // Since the server uses a self signed certificate we need a client that trusts all certificates
    SSLSocket socket = (SSLSocket) LDAPLoginModule.TrustAllSSLSocketFactory.getDefault()
      .createSocket(addr.getHostName(), addr.getPort());
    socket.setSoTimeout(5000); // in millis
    // Would throw exception if the server does not support ssl.
    // "javax.net.ssl.SSLException: Unrecognized SSL message, plaintext connection?"
    socket.startHandshake();
    appFabricServer.stopAndWait();
  }
}
