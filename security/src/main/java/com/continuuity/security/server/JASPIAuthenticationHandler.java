package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Inject;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.jaspi.JaspiAuthenticatorFactory;
import org.eclipse.jetty.util.security.Constraint;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.net.URL;
import java.util.HashMap;

/**
 *
 */
public class JASPIAuthenticationHandler extends ConstraintSecurityHandler {
  private final CConfiguration configuration;
  private static final String configBase = "security.authentication.method.";
  private static final String[] mandatoryConfigurables = new String[] { "debug", "hostname", "port", "userBaseDn",
                                                                               "userRdnAttribute", "userObjectClass" };
  private static final String[] optionalConfigurables = new String[] { "bindDn", "bindPassword", "userIdAttribute",
                                                                        "userPasswordAttribute", "roleBaseDn",
                                                                        "roleNameAttribute", "roleMemberAttribute",
                                                                        "roleObjectClass" };

  @Inject
  public JASPIAuthenticationHandler(CConfiguration configuration) throws Exception {
    super();
    this.configuration = configuration;

    Constraint constraint = new Constraint();
    constraint.setRoles(new String[] {"*"});
    constraint.setAuthenticate(true);

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    URL realmFile = getClass().getResource("/realm.properties");
    HashLoginService loginService = new HashLoginService();
    loginService.setConfig(realmFile.toExternalForm());
    loginService.loadUsers();

    DefaultIdentityService identityService = new DefaultIdentityService();

    loginService.setIdentityService(identityService);
    JaspiAuthenticatorFactory jaspiAuthenticatorFactory = new JaspiAuthenticatorFactory();
    jaspiAuthenticatorFactory.setLoginService(loginService);

    this.setStrict(false);
    this.setIdentityService(identityService);
    this.setAuthenticatorFactory(jaspiAuthenticatorFactory);
    this.setLoginService(loginService);

    this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
  }

  protected Configuration getConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("contextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
        map.put("authenticationMethod", "simple");
        map.put("forceBindingLogin", "true");

        for(String configurable : mandatoryConfigurables) {
          map.put(configurable, configuration.get(configBase.concat(configurable)));
        }

        for(String configurable : optionalConfigurables) {
          String value = configuration.get(configBase.concat(configurable));
          if (value != null) {
            map.put(configurable, value);
          }
        }
        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(configuration.get("security.authentication.method.className"),
                                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, map)
        };
      }
    };
  }
}
