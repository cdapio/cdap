package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.util.HashMap;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * An Authentication handler that authenticates against a LDAP server instance for External Authentication.
 */
public class LDAPAuthenticationHandler extends JAASAuthenticationHandler {
  private static final String[] mandatoryConfigurables = new String[] { "debug", "hostname", "port", "userBaseDn",
                                                                                "userRdnAttribute", "userObjectClass" };
  private static final String[] optionalConfigurables = new String[] { "bindDn", "bindPassword", "userIdAttribute",
                                                                      "userPasswordAttribute", "roleBaseDn",
                                                                      "roleNameAttribute", "roleMemberAttribute",
                                                                      "roleObjectClass" };

  /**
   * Create a new Authentication handler to use LDAP for external authentication.
   * @param configuration
   * @throws Exception
   */
  @Inject
  public LDAPAuthenticationHandler(CConfiguration configuration) throws Exception {
    super(configuration);
  }

  /**
   * Create a configuration from properties. Allows optional configurables.
   * @return
   */
  @Override
  protected Configuration getLoginModuleConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("contextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
        map.put("authenticationMethod", "simple");
        map.put("forceBindingLogin", "true");

        for (String configurable : mandatoryConfigurables) {
          String key = Constants.Security.AUTH_HANDLER_CONFIG_BASE.concat(configurable);
          String value = configuration.get(key);
          if (value == null) {
            String errorMessage = String.format("Mandatory configuration %s is not set.", key);
            throw Throwables.propagate(new RuntimeException(errorMessage));
          }
          map.put(configurable, value);
        }

        for (String configurable: optionalConfigurables) {
          String value = configuration.get(Constants.Security.AUTH_HANDLER_CONFIG_BASE.concat(configurable));
          if (value != null) {
            map.put(configurable, value);
          }
        }
        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(configuration.get(Constants.Security.LOGIN_MODULE_CLASS_NAME),
                                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, map)
        };
      }
    };
  }
}
