package com.continuuity.security.server;

import com.google.inject.Inject;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.util.security.Constraint;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;

/**
 *
 */
public class JAASAuthenticationHandler extends ConstraintSecurityHandler {

  @Inject
  public JAASAuthenticationHandler() throws Exception {
    super();

    Constraint constraint = new Constraint();
    constraint.setRoles(new String[] {"*"});
    constraint.setAuthenticate(true);

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    JAASLoginService jaasLoginService = new JAASLoginService();
    jaasLoginService.setLoginModuleName("ldaploginmodule");
    jaasLoginService.setConfiguration(getConfiguration());

    this.setStrict(false);
    this.setIdentityService(new DefaultIdentityService());
    this.setAuthenticator(new BasicAuthenticator());
    this.setLoginService(jaasLoginService);
    this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
  }

  private Configuration getConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("debug", "true");
        map.put("contextFactory", "com.sun.jndi.ldap.LdapCtxFactory");
        map.put("hostname", "crowd.continuuity.com");
        map.put("port", "389");
        map.put("authenticationMethod", "simple");
        map.put("forceBindingLogin", "true");
        map.put("userBaseDn", "ou=people,dc=continuuity,dc=com");
        map.put("userRdnAttribute", "cn");
        map.put("userObjectClass", "inetorgperson");
        return new AppConfigurationEntry[] {
          new AppConfigurationEntry("org.eclipse.jetty.plus.jaas.spi.LdapLoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,map)
        };
      }
    };
  }
}
