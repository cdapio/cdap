/*
 * Copyright © 2026 Cask Data, Inc.
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

import io.cdap.cdap.common.conf.Constants;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LdapAuthenticationHandler}.
 */
public class LdapAuthenticationHandlerTest {

  @After
  public void resetStaticVerifyCertificateFlag() {
    Map<String, String> props = createBaseMandatoryProps();
    props.put("useLdaps", "true");
    props.put("ldapsVerifyCertificate", "true");
    configureAndReadEntries(props);
  }

  @Test
  public void testLdapsFirstNonNullDefaultsAndOverrides() {
    // 1. useLdaps=true, ldapsVerifyCertificate="false" -> disables SSL certificate verification.
    Map<String, String> disableVerifyProps = createBaseMandatoryProps();
    disableVerifyProps.put("useLdaps", "true");
    disableVerifyProps.put("ldapsVerifyCertificate", "false");
    AppConfigurationEntry[] entries = configureAndReadEntries(disableVerifyProps);
    Assert.assertEquals(1, entries.length);
    Assert.assertEquals(LdapLoginModule.class.getName(), entries[0].getLoginModuleName());
    Assert.assertFalse(LdapAuthenticationHandler.getLdapSslVerifyCertificate());

    // 2. useLdaps=null -> defaults to "false" via firstNonNull, so verify flag stays unchanged (false).
    Map<String, String> nullUseLdapsProps = createBaseMandatoryProps();
    nullUseLdapsProps.put("ldapsVerifyCertificate", "true");
    configureAndReadEntries(nullUseLdapsProps);
    Assert.assertFalse(LdapAuthenticationHandler.getLdapSslVerifyCertificate());

    // 3. useLdaps="true", ldapsVerifyCertificate=null -> defaults to "true" via firstNonNull.
    Map<String, String> nullVerifyProps = createBaseMandatoryProps();
    nullVerifyProps.put("useLdaps", "true");
    configureAndReadEntries(nullVerifyProps);
    Assert.assertTrue(LdapAuthenticationHandler.getLdapSslVerifyCertificate());
  }

  @Test(expected = RuntimeException.class)
  public void testMissingMandatoryPropertyThrows() {
    Map<String, String> props = createBaseMandatoryProps();
    props.remove("hostname");
    configureAndReadEntries(props);
  }

  private static AppConfigurationEntry[] configureAndReadEntries(Map<String, String> props) {
    LdapAuthenticationHandler handler = new LdapAuthenticationHandler();
    handler.handlerProps = props;
    Configuration configuration = handler.getLoginModuleConfiguration();
    return configuration.getAppConfigurationEntry("jaas");
  }

  private static Map<String, String> createBaseMandatoryProps() {
    Map<String, String> props = new HashMap<>();
    props.put(Constants.Security.LOGIN_MODULE_CLASS_NAME, LdapLoginModule.class.getName());
    props.put("debug", "false");
    props.put("hostname", "127.0.0.1");
    props.put("port", "389");
    props.put("userBaseDn", "dc=example,dc=com");
    props.put("userRdnAttribute", "cn");
    props.put("userObjectClass", "inetorgperson");
    return props;
  }
}
