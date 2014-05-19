package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Inject;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.util.security.Constraint;

import javax.security.auth.login.Configuration;

/**
 * An abstract authentication handler that supports the JAAS interface for external authentication.
 */
public abstract class JAASAuthenticationHandler extends ConstraintSecurityHandler {

  @Inject
  public JAASAuthenticationHandler(String loginModuleName, CConfiguration configuration) throws Exception {
    super();

    Constraint constraint = new Constraint();
    constraint.setRoles(new String[] {"*"});
    constraint.setAuthenticate(true);

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    JAASLoginService jaasLoginService = new JAASLoginService();
    jaasLoginService.setLoginModuleName(loginModuleName);
    jaasLoginService.setConfiguration(getLoginModuleConfiguration());

    this.setStrict(false);
    this.setIdentityService(new DefaultIdentityService());
    this.setAuthenticator(new BasicAuthenticator());
    this.setLoginService(jaasLoginService);
    this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
  }

  protected abstract Configuration getLoginModuleConfiguration();
}
