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
import co.cask.cdap.common.utils.Networks;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.eclipse.jetty.plus.jaas.spi.PropertyFileLoginModule;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import static org.junit.Assert.assertEquals;

/**
 * Base test class for Mutual TLS Based ExternalAuthenticationServer.
 */
public abstract class ExternalMTLSAuthenticationServerTestBase extends ExternalAuthenticationServerTestBase  {

  protected abstract KeyManager[] getInvalidKeyManagers() throws Exception;

  protected abstract KeyManager[] getKeyManagers() throws Exception;

  protected abstract TrustManager[] getTrustManagers() throws Exception;

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

    URL keytabUrl = ExternalMTLSAuthenticationServerTestBase.class.getClassLoader().getResource("test.keytab");
    Assert.assertNotNull(keytabUrl);
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, keytabUrl.getPath());
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "test_principal");
    return cConf;
  }

  @Override
  protected void startExternalAuthenticationServer() throws Exception {
    // no-op
  }

  @Override
  protected void stopExternalAuthenticationServer() throws Exception {
    // no-op
  }

  /**
   * Return a Http Client setup with the Default Client's keystore & TrustStore
   *
   * @return
   * @throws Exception
   */
  protected HttpClient getHTTPClient() throws Exception {
    return getHTTPClient(getKeyManagers(), getTrustManagers());
  }

  private HttpClient getHTTPClient(KeyManager[] kms, TrustManager[] tms) throws Exception {
    SSLContext sslContext = SSLContext.getInstance("SSL");
    sslContext.init(kms, tms, new SecureRandom());
    // only for test purposes ignoring check of certificate hostname matching host on which server runs
    SSLSocketFactory sf = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
    Scheme httpsScheme = new Scheme("https", getAuthServerPort(), sf);
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(httpsScheme);
    // Apache HttpClient version >4.2 should use BasicClientConnectionManager
    ClientConnectionManager cm = new BasicClientConnectionManager(schemeRegistry);
    return new DefaultHttpClient(cm);
  }

  /**
   * Test request to server using a client certificate that is not trusted by the server.
   *
   * @throws Exception
   */
  @Override
  @Test
  public void testInvalidAuthentication() throws Exception {
    HttpClient client = getHTTPClient(getInvalidKeyManagers(), getTrustManagers());
    String uri = String.format("%s://%s:%d/%s", getProtocol(), getServer().getSocketAddress().getAddress()
                                 .getHostAddress(),
                               getServer().getSocketAddress().getPort(), GrantAccessToken.Paths.GET_TOKEN);

    HttpGet request = new HttpGet(uri);
    HttpResponse response = client.execute(request);

    // Request is Unauthorized
    assertEquals(403, response.getStatusLine().getStatusCode());
  }


  /**
   * Test request to server using a client certificate that is not trusted by the server.
   *
   * @throws Exception
   */
  @Test
  public void testInvalidClientCertForStatusEndpoint() throws Exception {
    HttpClient client = getHTTPClient(getInvalidKeyManagers(), getTrustManagers());
    String uri = String.format("%s://%s:%d/%s", getProtocol(), getServer().getSocketAddress().getAddress()
                                 .getHostAddress(),
                               getServer().getSocketAddress().getPort(), Constants.EndPoints.STATUS);

    HttpGet request = new HttpGet(uri);
    HttpResponse response = client.execute(request);
    // Request is Authorized
    assertEquals(200, response.getStatusLine().getStatusCode());
  }


  /**
   * Test request to server without providing a client certificate
   *
   * @throws Exception
   */
  @Test
  public void testMissingClientCertAuthentication() throws Exception {
    HttpClient client = getHTTPClient(null, getTrustManagers());
    String uri = String.format("%s://%s:%d/%s", getProtocol(), getServer().getSocketAddress().getAddress()
                                 .getHostAddress(),
                               getServer().getSocketAddress().getPort(), GrantAccessToken.Paths.GET_TOKEN);
    HttpGet request = new HttpGet(uri);
    HttpResponse response = client.execute(request);
    // Status request is authorized without any extra headers
    assertEquals(403, response.getStatusLine().getStatusCode());
  }

}
