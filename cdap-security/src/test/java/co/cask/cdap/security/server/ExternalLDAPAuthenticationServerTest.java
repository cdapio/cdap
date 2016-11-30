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
import com.google.common.collect.Maps;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.util.Map;

/**
 * Tests for {@link ExternalAuthenticationServer}.
 */
public class ExternalLDAPAuthenticationServerTest extends ExternalLDAPAuthenticationServerTestBase {

  static ExternalLDAPAuthenticationServerTest testServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, "127.0.0.1");
    cConf.set(Constants.Security.SSL_ENABLED, "false");
    cConf.set(Constants.Security.AUTH_SERVER_BIND_PORT, "0");

    configuration = cConf;
    sConfiguration = SConfiguration.create();

    ldapListenerConfig = InMemoryListenerConfig.createLDAPConfig("LDAP", InetAddress.getByName("127.0.0.1"),
                                                                 ldapPort, null);
    testServer = new ExternalLDAPAuthenticationServerTest();
    testServer.setup();
  }


  @AfterClass
  public static void afterClass() throws Exception {
      testServer.tearDown();
  }

  @Override
  protected String getProtocol() {
    return "http";
  }

  @Override
  protected HttpClient getHTTPClient() throws Exception {
    return new DefaultHttpClient();
  }

  @Override
  protected Map<String, String> getAuthRequestHeader() throws Exception {
    Map headers = Maps.newHashMap();
    headers.put("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    return headers;
  }

  @Override
  protected String getAuthenticatedUserName() throws Exception {
   return "admin";
  }
}
