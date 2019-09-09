/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
import io.cdap.cdap.common.security.KeyStores;
import io.cdap.cdap.common.security.KeyStoresTest;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.security.auth.AccessTokenTransformer;
import io.cdap.cdap.security.guice.SecurityModules;
import io.cdap.common.http.HttpRequests;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import javax.net.SocketFactory;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

/**
 * Tests Netty Router running on HTTPS.
 */
@RunWith(Parameterized.class)
public class NettyRouterHttpsTest extends NettyRouterTestBase {

  private final CConfiguration cConf;
  private final SConfiguration sConf;

  @Parameterized.Parameters(name = "{index}: NettyRouterHttpsTest(useKeyStore = {0})")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {true},
      {false}
    });
  }

  /**
   * Creates a new instance of the test.
   *
   * @param useKeyStore true if Keystore file path is used; other cert PEM file path will be used.
   * @see NettyRouter
   */
  public NettyRouterHttpsTest(boolean useKeyStore) throws Exception {
    this.cConf = CConfiguration.create();
    this.sConf = SConfiguration.create();

    cConf.setBoolean(Constants.Security.SSL.EXTERNAL_ENABLED, true);
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);
    cConf.setInt(Constants.Router.CONNECTION_TIMEOUT_SECS, CONNECTION_IDLE_TIMEOUT_SECS);

    String keyStorePass = sConf.get(Constants.Security.Router.SSL_KEYSTORE_PASSWORD);
    KeyStore keyStore = KeyStores.generatedCertKeyStore(1, keyStorePass);

    if (useKeyStore) {
      File keyStoreFile = TEMP_FOLDER.newFile();
      try (OutputStream os = new FileOutputStream(keyStoreFile)) {
        keyStore.store(os, sConf.get(Constants.Security.Router.SSL_KEYPASSWORD).toCharArray());
      }
      sConf.set(Constants.Security.Router.SSL_KEYSTORE_PATH, keyStoreFile.getAbsolutePath());
    } else {
      File pemFile = KeyStoresTest.writePEMFile(TEMP_FOLDER.newFile(), keyStore,
                                                keyStore.aliases().nextElement(), keyStorePass);
      cConf.set(Constants.Security.Router.SSL_CERT_PATH, pemFile.getAbsolutePath());
      sConf.set(Constants.Security.Router.SSL_CERT_PASSWORD, keyStorePass);
    }
  }

  @Override
  protected RouterService createRouterService(String hostname, DiscoveryService discoveryService) {
    return new HttpsRouterService(cConf, sConf, hostname, discoveryService);
  }

  @Override
  protected String getProtocol() {
    return "https";
  }

  @Override
  protected HttpURLConnection openURL(URL url) throws Exception {
    HttpsURLConnection urlConn = (HttpsURLConnection) url.openConnection();
    HttpRequests.disableCertCheck(urlConn);
    return urlConn;
  }

  @Override
  protected DefaultHttpClient getHTTPClient() throws Exception {
    SSLContext sslContext = SSLContext.getInstance("TLS");

    // set up a TrustManager that trusts everything
    sslContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), new SecureRandom());

    SSLSocketFactory sf = new SSLSocketFactory(sslContext, new AllowAllHostnameVerifier());
    Scheme httpsScheme = new Scheme("https", 10101, sf);
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(httpsScheme);

    // apache HttpClient version >4.2 should use BasicClientConnectionManager
    ClientConnectionManager cm = new BasicClientConnectionManager(schemeRegistry);
    return new DefaultHttpClient(cm);
  }

  @Override
  protected SocketFactory getSocketFactory() throws Exception {
    SSLContext sc = SSLContext.getInstance("TLS");
    sc.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), new SecureRandom());
    return sc.getSocketFactory();
  }

  private static class HttpsRouterService extends RouterService {

    private final CConfiguration cConf;
    private final SConfiguration sConf;
    private final String hostname;
    private final DiscoveryService discoveryService;

    private NettyRouter router;

    private HttpsRouterService(CConfiguration cConf, SConfiguration sConf,
                               String hostname, DiscoveryService discoveryService) {
      this.cConf = CConfiguration.copy(cConf);
      this.sConf = SConfiguration.copy(sConf);
      this.hostname = hostname;
      this.discoveryService = discoveryService;
    }

    @Override
    protected void startUp() {
      Injector injector = Guice.createInjector(new SecurityModules().getInMemoryModules(),
                                               new InMemoryDiscoveryModule(),
                                               new AppFabricTestModule(cConf));
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      AccessTokenTransformer accessTokenTransformer = injector.getInstance(AccessTokenTransformer.class);
      cConf.set(Constants.Router.ADDRESS, hostname);

      router =
        new NettyRouter(cConf, sConf, InetAddresses.forString(hostname),
                        new RouterServiceLookup(cConf, (DiscoveryServiceClient) discoveryService,
                                                new RouterPathLookup()),
                        new SuccessTokenValidator(), accessTokenTransformer, discoveryServiceClient);
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
