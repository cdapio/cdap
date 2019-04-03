/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * An Authentication handler that authenticates against a LDAP server instance for External Authentication.
 */
public class LDAPAuthenticationHandler extends JAASAuthenticationHandler {

  private static final List<String> mandatoryConfigurables = ImmutableList.of("debug", "hostname", "port", "userBaseDn",
                                                                              "userRdnAttribute", "userObjectClass");

  private static final List<String> optionalConfigurables = ImmutableList.of("bindDn", "bindPassword", "useLdaps",
                                                                             "userIdAttribute", "userPasswordAttribute",
                                                                             "roleBaseDn", "roleNameAttribute",
                                                                             "roleMemberAttribute", "roleObjectClass");

  private static boolean ldapSSLVerifyCertificate = true;

  /**
   * Create a configuration from properties. Allows optional configurables.
   */
  @Override
  protected Configuration getLoginModuleConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        Map<String, String> map = new HashMap<>();
        map.put("contextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
        map.put("authenticationMethod", "simple");
        map.put("forceBindingLogin", "true");

        copyProperties(handlerProps, map, mandatoryConfigurables, true);
        copyProperties(handlerProps, map, optionalConfigurables, false);

        String ldapsVerifyCertificate = handlerProps.get("ldapsVerifyCertificate");
        ldapSSLVerifyCertificate = Boolean.parseBoolean(Objects.firstNonNull(ldapsVerifyCertificate, "true"));

        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(handlerProps.get(Constants.Security.LOGIN_MODULE_CLASS_NAME),
                                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, map)
        };
      }
    };
  }

  private void copyProperties(Map<String, String> fromMap, Map<String, String> toMap,
                              List<String> keys, boolean mandatory) {
    for (String key : keys) {
      String value = fromMap.get(key);
      if (value != null) {
        toMap.put(key, value);
      } else if (mandatory) {
        throw new RuntimeException(String.format("Mandatory configuration %s is not set.", key));
      }
    }
  }

  static boolean getLdapSSLVerifyCertificate() {
    return ldapSSLVerifyCertificate;
  }
}
