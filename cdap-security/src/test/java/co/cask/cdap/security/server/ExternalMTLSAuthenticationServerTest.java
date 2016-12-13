/*
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
package co.cask.cdap.security.server;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.FileInputStream;

import java.net.URL;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Tests for Mutual TLS a.k.a 2-way SSL Based Auth
 * <p>
 * <p>
 * For these tests we will be using key store & trust store that have been pre-created. For the purpose of the test
 * we will be using self-signed certificates.
 * The Server's trust store contains the client's certificate & and the client's trust store contains the server's
 * certificate
 */
public class ExternalMTLSAuthenticationServerTest extends ExternalMTLSAuthenticationServerTestBase {

  static final String AUTH_HANDLER_CONFIG_BASE = Constants.Security.AUTH_HANDLER_CONFIG_BASE;
  static ExternalMTLSAuthenticationServerTest testServer;
  static String validClientCN = "client";


  @BeforeClass
  public static void beforeClass() throws Exception {
    URL serverTrustoreURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("server-trust" +
                                                                                                      ".jks");
    URL serverKeystoreURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("server-key.jks");
    URL realmURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("realm.properties");

    Assert.assertNotNull(serverTrustoreURL);
    Assert.assertNotNull(serverKeystoreURL);
    Assert.assertNotNull(realmURL);

    CConfiguration cConf = CConfiguration.create();
    SConfiguration sConf = SConfiguration.create();
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, "127.0.0.1");

    // enables SSL
    cConf.set(Constants.Security.SSL.EXTERNAL_ENABLED, "true");
    cConf.set(Constants.Security.AuthenticationServer.SSL_PORT, "0");

    // set up port for non-ssl endpoints
    cConf.set(Constants.Security.AUTH_SERVER_BIND_PORT, "1");

    // Configure the Custom Handler
    cConf.set(AUTH_HANDLER_CONFIG_BASE.concat("ClassName"), "co.cask.cdap.security.server" +
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
   * Sets up the client's keystore Using a certificate that is not part of the server's trustore
   *
   * @return
   * @throws Exception
   */
  @Override
  protected KeyManager[] getInvalidKeyManagers() throws Exception {
    URL clientKeystoreURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("invalid-client" +
                                                                                                      ".jks");
    Assert.assertNotNull(clientKeystoreURL);
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    KeyStore ks = KeyStore.getInstance("JKS");
    char[] ksPass = "secret".toCharArray();
    FileInputStream fis = new FileInputStream(clientKeystoreURL.getPath());
    ks.load(fis, ksPass);
    kmf.init(ks, configuration.get("security.auth.server.ssl.keystore.password", "secret").toCharArray());

    fis.close();
    fis = null;

    return kmf.getKeyManagers();
  }


  /**
   * Sets up the client's keystore from the client_keystore_path
   *
   * @return
   * @throws Exception
   */
  @Override
  protected KeyManager[] getKeyManagers() throws Exception {
    URL clientKeystoreURL = ExternalMTLSAuthenticationServerTest.class.getClassLoader().getResource("client-key.jks");
    Assert.assertNotNull(clientKeystoreURL);
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    KeyStore ks = KeyStore.getInstance("JKS");
    char[] ksPass = "secret".toCharArray();
    FileInputStream fis = new FileInputStream(clientKeystoreURL.getPath());
    ks.load(fis, ksPass);
    kmf.init(ks, configuration.get("security.auth.server.ssl.keystore.password", "secret").toCharArray());

    fis.close();
    fis = null;

    return kmf.getKeyManagers();
  }

  /**
   * For the test trusting any cert the server provides
   *
   * @return
   * @throws Exception
   */
  @Override
  protected TrustManager[] getTrustManagers() throws Exception {
    // Setting up the client's trust store, which trusts all certs
    TrustManager[] tms = new TrustManager[]{new X509TrustManager() {
      @Override
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      @Override
      public void checkClientTrusted(java.security.cert.X509Certificate[] x509Certificates, String s)
        throws CertificateException {
        // no-op
      }

      @Override
      public void checkServerTrusted(java.security.cert.X509Certificate[] x509Certificates, String s)
        throws CertificateException {
        // no- op
      }
    }};

    return tms;
  }

  protected Map<String, String> getAuthRequestHeader() throws Exception {
    return null;
  }

  @Override
  protected String getAuthenticatedUserName() throws Exception {
    return validClientCN;
  }

}
