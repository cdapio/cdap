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

package co.cask.cdap.security.server;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.util.ssl.KeyStoreKeyManager;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Tests for {@link ExternalAuthenticationServer} with SSL enabled.
 */
public class ExternalAuthenticationServerSSLTest extends ExternalAuthenticationServerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    URL certUrl = ExternalAuthenticationServerSSLTest.class.getClassLoader().getResource("cert.jks");
    Assert.assertNotNull(certUrl);

    String authHandlerConfigBase = Constants.Security.AUTH_HANDLER_CONFIG_BASE;

    CConfiguration cConf = CConfiguration.create();
    SConfiguration sConf = SConfiguration.create();
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, "127.0.0.1");
    cConf.set(Constants.Security.SSL_ENABLED, "true");
    cConf.set(Constants.Security.AuthenticationServer.SSL_PORT, "0");
    cConf.set(authHandlerConfigBase.concat("useLdaps"), "true");
    cConf.set(authHandlerConfigBase.concat("ldapsVerifyCertificate"), "false");
    sConf.set(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PATH, certUrl.getPath());
    configuration = cConf;
    sConfiguration = sConf;

    String keystorePassword = sConf.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PASSWORD);
    KeyStoreKeyManager keyManager = new KeyStoreKeyManager(certUrl.getFile(), keystorePassword.toCharArray());
    SSLUtil sslUtil = new SSLUtil(keyManager, new TrustAllTrustManager());
    ldapListenerConfig = InMemoryListenerConfig.createLDAPSConfig("LDAP", InetAddress.getByName("127.0.0.1"),
                                                                  ldapPort, sslUtil.createSSLServerSocketFactory(),
                                                                  sslUtil.createSSLSocketFactory());

    setup();
  }

  @Override
  protected String getProtocol() {
    return "https";
  }

  @Override
  protected HttpClient getHTTPClient() throws Exception {
    SSLContext sslContext = SSLContext.getInstance("SSL");

    // set up a TrustManager that trusts everything
    sslContext.init(null, new TrustManager[] { new X509TrustManager() {
      @Override
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      @Override
      public void checkClientTrusted(java.security.cert.X509Certificate[] x509Certificates, String s)
        throws CertificateException {
        //
      }

      @Override
      public void checkServerTrusted(java.security.cert.X509Certificate[] x509Certificates, String s)
        throws CertificateException {
        //
      }

    } }, new SecureRandom());

    SSLSocketFactory sf = new SSLSocketFactory(sslContext);
    Scheme httpsScheme = new Scheme("https", getAuthServerPort(), sf);
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(httpsScheme);

    // apache HttpClient version >4.2 should use BasicClientConnectionManager
    ClientConnectionManager cm = new BasicClientConnectionManager(schemeRegistry);
    return new DefaultHttpClient(cm);
  }
}
