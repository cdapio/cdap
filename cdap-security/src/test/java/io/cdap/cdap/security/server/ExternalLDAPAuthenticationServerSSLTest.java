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

package io.cdap.cdap.security.server;

import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.util.ssl.KeyStoreKeyManager;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.security.HttpsEnabler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;

/**
 * Tests for {@link ExternalAuthenticationServer} with SSL enabled.
 */
public class ExternalLDAPAuthenticationServerSSLTest extends ExternalLDAPAuthenticationServerTestBase {

  private static ExternalLDAPAuthenticationServerSSLTest testServer;
  private static final HttpsEnabler HTTPS_ENABLER = new HttpsEnabler().setTrustAll(true);

  @BeforeClass
  public static void beforeClass() throws Exception {
    URL certUrl = ExternalLDAPAuthenticationServerSSLTest.class.getClassLoader().getResource("cert.jks");
    Assert.assertNotNull(certUrl);

    String authHandlerConfigBase = Constants.Security.AUTH_HANDLER_CONFIG_BASE;

    CConfiguration cConf = CConfiguration.create();
    SConfiguration sConf = SConfiguration.create();
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, InetAddress.getLoopbackAddress().getHostName());
    cConf.set(Constants.Security.SSL.EXTERNAL_ENABLED, "true");
    cConf.setInt(Constants.Security.AuthenticationServer.SSL_PORT, 0);
    cConf.set(authHandlerConfigBase.concat("useLdaps"), "true");
    cConf.set(authHandlerConfigBase.concat("ldapsVerifyCertificate"), "false");
    sConf.set(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PATH, certUrl.getPath());
    configuration = cConf;
    sConfiguration = sConf;

    String keystorePassword = sConf.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PASSWORD);
    KeyStoreKeyManager keyManager = new KeyStoreKeyManager(certUrl.getFile(), keystorePassword.toCharArray());
    SSLUtil sslUtil = new SSLUtil(keyManager, new TrustAllTrustManager());
    ldapListenerConfig = InMemoryListenerConfig.createLDAPSConfig("LDAP", InetAddress.getLoopbackAddress(),
                                                                  ldapPort, sslUtil.createSSLServerSocketFactory(),
                                                                  sslUtil.createSSLSocketFactory());

    testServer = new ExternalLDAPAuthenticationServerSSLTest();
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

  @Override
  protected HttpURLConnection openConnection(URL url) throws Exception {
    return HTTPS_ENABLER.enable((HttpsURLConnection) super.openConnection(url));
  }

  @Override
  protected Map<String, String> getAuthRequestHeader() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    return headers;
  }

  @Override
  protected String getAuthenticatedUserName() {
    return "admin";
  }
}
