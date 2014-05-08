package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Inject;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;

/**
 *
 */
public class LDAPAuthenticationHandler extends JAASAuthenticationHandler {
  private final CConfiguration configuration;

  @Inject
  public LDAPAuthenticationHandler(CConfiguration configuration) throws Exception {
    super("ldaploginmodule");
    this.configuration = configuration;
  }

  @Override
  protected Configuration getConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("contextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
        map.put("authenticationMethod", "simple");
        map.put("forceBindingLogin", "true");
        map.put("debug", configuration.get("security.authentication.method.debug"));
        map.put("hostname", configuration.get("security.authentication.method.hostname"));
        map.put("port", configuration.get("security.authentication.method.port"));
        map.put("userBaseDn", configuration.get("security.authentication.method.userBaseDn"));
        map.put("userRdnAttribute", configuration.get("security.authentication.method.userRdnAttribute"));
        map.put("userObjectClass", configuration.get("security.authentication.method.userObjectClass"));
        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(configuration.get("security.authentication.method.className"),
                                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, map)
        };
      }
    };
  }
}
