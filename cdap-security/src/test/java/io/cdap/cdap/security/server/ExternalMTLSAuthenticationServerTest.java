/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.security.server;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.common.utils.Networks;
import org.eclipse.jetty.plus.jaas.spi.PropertyFileLoginModule;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;

import static org.junit.Assert.assertEquals;

/**
 * Tests for Mutual TLS a.k.a 2-way SSL Based Auth
 * <p>
 * <p>
 * For these tests we will be using key store & trust store that have been pre-created. For the purpose of the test
 * we will be using self-signed certificates.
 * The Server's trust store contains the client's certificate & and the client's trust store contains the server's
 * certificate
 */
public class ExternalMTLSAuthenticationServerTest extends ExternalAuthenticationServerTestBase  {

  private static final String AUTH_HANDLER_CONFIG_BASE = Constants.Security.AUTH_HANDLER_CONFIG_BASE;
  private static final String VALID_CLIENT_CN = "client";
  private static ExternalMTLSAuthenticationServerTest testServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    URL serverTrustoreURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("server-trust.jks");
    URL serverKeystoreURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("server-key.jks");
    URL realmURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("realm.properties");

    Assert.assertNotNull(serverTrustoreURL);
    Assert.assertNotNull(serverKeystoreURL);
    Assert.assertNotNull(realmURL);

    CConfiguration cConf = CConfiguration.create();
    SConfiguration sConf = SConfiguration.create();
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, InetAddress.getLoopbackAddress().getHostName());

    // enables SSL
    cConf.set(Constants.Security.SSL.EXTERNAL_ENABLED, "true");
    cConf.setInt(Constants.Security.AuthenticationServer.SSL_PORT, 0);

    // set up port for non-ssl endpoints
    cConf.set(Constants.Security.AUTH_SERVER_BIND_PORT, "1");

    // Configure the Custom Handler
    cConf.set(AUTH_HANDLER_CONFIG_BASE.concat("ClassName"), "io.cdap.cdap.security.server" +
      ".CertificateAuthenticationHandler");

    // setup the realm file for Identity
    cConf.set(AUTH_HANDLER_CONFIG_BASE.concat("realmfile"), realmURL.getPath());

    cConf.set(Constants.Security.AuthenticationServer.SSL_TRUSTSTORE_PATH, serverTrustoreURL.getPath());
    cConf.set(Constants.Security.AuthenticationServer.SSL_TRUSTSTORE_PASSWORD, "secret");
    cConf.set(Constants.Security.AuthenticationServer.SSL_TRUSTSTORE_TYPE, "JKS");

    // Setup the Server's Key Store
    cConf.set(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PATH, serverKeystoreURL.getPath());
    sConf.set(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PATH, serverKeystoreURL.getPath());

    sConf.set(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PASSWORD, "secret");
    sConf.set(Constants.Security.AuthenticationServer.SSL_KEYPASSWORD, "secret");
    sConf.set(Constants.Security.AuthenticationServer.SSL_KEYSTORE_TYPE, "JKS");

    configuration = cConf;
    sConfiguration = sConf;

    testServer = new ExternalMTLSAuthenticationServerTest();
    testServer.setup();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testServer.tearDown();
  }

  @Override
  protected String getProtocol() {
    return "https";
  }

  /**
   * Authentication Server with 2-way SSL and related handler configurations.
   */
  protected CConfiguration getConfiguration(CConfiguration cConf) {
    String configBase = Constants.Security.AUTH_HANDLER_CONFIG_BASE;
    cConf.set(Constants.Security.SSL.EXTERNAL_ENABLED, Boolean.TRUE.toString());

    // Use random port for testing
    cConf.setInt(Constants.Security.AUTH_SERVER_BIND_PORT, Networks.getRandomPort());
    cConf.setInt(Constants.Security.AuthenticationServer.SSL_PORT, Networks.getRandomPort());

    // Setting the Authentication Handler to the Certificate Handler
    cConf.set(Constants.Security.AUTH_HANDLER_CLASS, CertificateAuthenticationHandler.class.getName());
    cConf.set(Constants.Security.LOGIN_MODULE_CLASS_NAME, PropertyFileLoginModule.class.getName());
    cConf.set(configBase.concat("debug"), "true");
    cConf.set(configBase.concat("hostname"), "localhost");

    URL keytabUrl = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("test.keytab");
    Assert.assertNotNull(keytabUrl);
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, keytabUrl.getPath());
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "test_principal");
    return cConf;
  }

  @Override
  protected void startExternalAuthenticationServer() {
    // no-op
  }

  @Override
  protected void stopExternalAuthenticationServer() {
    // no-op
  }

  @Override
  protected HttpURLConnection openConnection(URL url) throws Exception {
    return openConnection(url, "client-key.jks");
  }

  private HttpsURLConnection openConnection(URL url, String keyStoreResource) throws Exception {
    HttpsURLConnection urlConn = (HttpsURLConnection) super.openConnection(url);

    URL clientKeystoreURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource(keyStoreResource);
    Assert.assertNotNull(clientKeystoreURL);
    KeyStore ks = KeyStore.getInstance("JKS");

    try (InputStream is = clientKeystoreURL.openConnection().getInputStream()) {
      ks.load(is, "secret".toCharArray());
    }
    return new HttpsEnabler().setKeyStore(ks, () -> configuration.get("security.auth.server.ssl.keystore.password",
                                                                      "secret").toCharArray())
      .setTrustAll(true)
      .enable(urlConn);
  }

  /**
   * Test request to server using a client certificate that is not trusted by the server.
   *
   * @throws Exception
   */
  @Override
  @Test
  public void testInvalidAuthentication() throws Exception {
    HttpsURLConnection urlConn = openConnection(getURL(GrantAccessToken.Paths.GET_TOKEN), "invalid-client.jks");
    try {
      // Request is Unauthorized
      assertEquals(403, urlConn.getResponseCode());
    } finally {
      urlConn.disconnect();
    }
  }


  /**
   * Test request to server using a client certificate that is not trusted by the server.
   *
   * @throws Exception
   */
  @Test
  public void testInvalidClientCertForStatusEndpoint() throws Exception {
    HttpsURLConnection urlConn = openConnection(getURL(Constants.EndPoints.STATUS), "invalid-client.jks");
    try {
      // Request is Authorized
      assertEquals(200, urlConn.getResponseCode());
    } finally {
      urlConn.disconnect();
    }
  }


  /**
   * Test request to server without providing a client certificate
   *
   * @throws Exception
   */
  @Test
  public void testMissingClientCertAuthentication() throws Exception {
    HttpsURLConnection urlConn = new HttpsEnabler()
      .setTrustAll(true)
      .enable((HttpsURLConnection) openConnection(getURL(GrantAccessToken.Paths.GET_TOKEN)));

    try {
      // Status request is authorized without any extra headers
      assertEquals(403, urlConn.getResponseCode());
    } finally {
      urlConn.disconnect();
    }
  }

  protected Map<String, String> getAuthRequestHeader() {
    return Collections.emptyMap();
  }

  @Override
  protected String getAuthenticatedUserName() {
    return VALID_CLIENT_CN;
  }
}
