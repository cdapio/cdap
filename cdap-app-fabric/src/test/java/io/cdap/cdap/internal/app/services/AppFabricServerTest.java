/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.security.server.LDAPLoginModule;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.SSLSocket;

/**
 *
 */
public class AppFabricServerTest {

  @Test
  public void startStopServer() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    try {
      AppFabricServer server = injector.getInstance(AppFabricServer.class);
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      Service.State state = server.startAndWait();
      Assert.assertSame(state, Service.State.RUNNING);

      final EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
        () -> discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP));
      Assert.assertNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS));

      state = server.stopAndWait();
      Assert.assertSame(state, Service.State.TERMINATED);

      Tasks.waitFor(true, () -> endpointStrategy.pick() == null, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      AppFabricTestHelper.shutdown();
    }
  }

  @Test
  public void testSSL() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, true);
    cConf.setInt(Constants.AppFabric.SERVER_SSL_PORT, 0);
    SConfiguration sConf = SConfiguration.create();
    final Injector injector = AppFabricTestHelper.getInjector(cConf, sConf);
    try {
      final DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      AppFabricServer appFabricServer = injector.getInstance(AppFabricServer.class);
      appFabricServer.startAndWait();
      Assert.assertTrue(appFabricServer.isRunning());

      Supplier<EndpointStrategy> endpointStrategySupplier = Suppliers.memoize(
        () -> new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP)))
        ::get;
      Discoverable discoverable = endpointStrategySupplier.get().pick(3, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverable);
      Assert.assertTrue(URIScheme.HTTPS.isMatch(discoverable));
      InetSocketAddress addr = discoverable.getSocketAddress();
      // Since the server uses a self signed certificate we need a client that trusts all certificates
      SSLSocket socket = (SSLSocket) LDAPLoginModule.TrustAllSSLSocketFactory.getDefault()
        .createSocket(addr.getHostName(), addr.getPort());
      socket.setSoTimeout(5000); // in millis
      // Would throw exception if the server does not support ssl.
      // "javax.net.ssl.SSLException: Unrecognized SSL message, plaintext connection?"
      socket.startHandshake();
      appFabricServer.stopAndWait();
    } finally {
      AppFabricTestHelper.shutdown();
    }
  }
}
