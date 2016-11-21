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

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.Entry;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

import static org.mockito.Mockito.mock;


/**
 * Base test class for LDAP Based ExternalAuthenticationServer.
 */
public abstract class ExternalLDAPAuthenticationServerTestBase extends ExternalAuthenticationServerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalLDAPAuthenticationServerTestBase.class);
  private static InMemoryDirectoryServer ldapServer;
  protected static int ldapPort = Networks.getRandomPort();

  private static final Logger TEST_AUDIT_LOGGER = mock(Logger.class);

  protected static InMemoryListenerConfig ldapListenerConfig;

  /**
   * LDAP server and related handler configurations.
   */
  protected CConfiguration getConfiguration(CConfiguration cConf) {
    String configBase = Constants.Security.AUTH_HANDLER_CONFIG_BASE;

    // Use random port for testing
    cConf.setInt(Constants.Security.AUTH_SERVER_BIND_PORT, Networks.getRandomPort());
    cConf.setInt(Constants.Security.AuthenticationServer.SSL_PORT, Networks.getRandomPort());

    cConf.set(Constants.Security.AUTH_HANDLER_CLASS, LDAPAuthenticationHandler.class.getName());
    cConf.set(Constants.Security.LOGIN_MODULE_CLASS_NAME, LDAPLoginModule.class.getName());
    cConf.set(configBase.concat("debug"), "true");
    cConf.set(configBase.concat("hostname"), "localhost");
    cConf.set(configBase.concat("port"), Integer.toString(ldapPort));
    cConf.set(configBase.concat("userBaseDn"), "dc=example,dc=com");
    cConf.set(configBase.concat("userRdnAttribute"), "cn");
    cConf.set(configBase.concat("userObjectClass"), "inetorgperson");

    URL keytabUrl = ExternalAuthenticationServerTestBase.class.getClassLoader().getResource("test.keytab");
    Assert.assertNotNull(keytabUrl);
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, keytabUrl.getPath());
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "test_principal");
    return cConf;
  }

  protected void startExternalAuthenticationServer() throws Exception {
    InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
    config.setListenerConfigs(ldapListenerConfig);

    Entry defaultEntry = new Entry(
      "dn: dc=example,dc=com",
      "objectClass: top",
      "objectClass: domain",
      "dc: example");
    Entry userEntry = new Entry(
      "dn: uid=user,dc=example,dc=com",
      "objectClass: inetorgperson",
      "cn: admin",
      "sn: User",
      "uid: user",
      "userPassword: realtime");

    ldapServer = new InMemoryDirectoryServer(config);
    ldapServer.addEntries(defaultEntry, userEntry);
    ldapServer.startListening();
  }

  protected void stopExternalAuthenticationServer() throws Exception {
    ldapServer.shutDown(true);
  }


}
