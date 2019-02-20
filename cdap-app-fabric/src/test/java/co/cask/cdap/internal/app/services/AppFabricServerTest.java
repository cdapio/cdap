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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.security.server.LDAPLoginModule;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.net.ssl.SSLSocket;

/**
 *
 */
public class AppFabricServerTest {

  @Test
  public void startStopServer() throws Exception {
    Injector injector = getInjector(CConfiguration.create(), null);
    AppFabricServer server = injector.getInstance(AppFabricServer.class);
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Service.State state = server.startAndWait();
    Assert.assertEquals(Service.State.RUNNING, state);

    final EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP));
    Assert.assertNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS));

    state = server.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, state);

    Tasks.waitFor(true, () -> endpointStrategy.pick() == null, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testSSL() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, true);
    cConf.setInt(Constants.AppFabric.SERVER_SSL_PORT, 0);
    SConfiguration sConf = SConfiguration.create();
    Injector injector = getInjector(cConf, sConf);

    final DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    AppFabricServer appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    Assert.assertTrue(appFabricServer.isRunning());

    Supplier<EndpointStrategy> endpointStrategySupplier = Suppliers.memoize(
      () -> new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP)))::get;
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

  private Injector getInjector(CConfiguration cConf, @Nullable SConfiguration sConf) {
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf, sConf));
    injector.getInstance(TransactionManager.class).startAndWait();
    // Register the tables before services will need to use them
    StructuredTableAdmin tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    try {
      structuredTableRegistry.initialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      StoreDefinition.createAllTables(tableAdmin, structuredTableRegistry);
    } catch (IOException | TableAlreadyExistsException e) {
      throw new RuntimeException("Failed to create the system tables", e);
    }
    injector.getInstance(DatasetService.class).startAndWait();
    return injector;
  }
}
