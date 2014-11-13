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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

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
   * Create a new Authentication handler to use LDAP for external authentication.
   */
  @Inject
  public LDAPAuthenticationHandler(CConfiguration configuration) throws Exception {
    super(configuration);
  }

  /**
   * Create a configuration from properties. Allows optional configurables.
   */
  @Override
  protected Configuration getLoginModuleConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        Map<String, String> map = Maps.newHashMap();
        map.put("contextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
        map.put("authenticationMethod", "simple");
        map.put("forceBindingLogin", "true");

        String authConfigBase = Constants.Security.AUTH_HANDLER_CONFIG_BASE;
        for (String configurable : mandatoryConfigurables) {
          String key = authConfigBase.concat(configurable);
          String value = configuration.get(key);
          if (value == null) {
            String errorMessage = String.format("Mandatory configuration %s is not set.", key);
            throw Throwables.propagate(new RuntimeException(errorMessage));
          }
          map.put(configurable, value);
        }

        for (String configurable: optionalConfigurables) {
          String value = configuration.get(authConfigBase.concat(configurable));
          if (value != null) {
            map.put(configurable, value);
          }
        }

        ldapSSLVerifyCertificate = configuration.getBoolean(authConfigBase.concat("ldapsVerifyCertificate"), true);

        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(configuration.get(Constants.Security.LOGIN_MODULE_CLASS_NAME),
                                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, map)
        };
      }
    };
  }

  static boolean getLdapSSLVerifyCertificate() {
    return ldapSSLVerifyCertificate;
  }
}
