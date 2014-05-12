package com.continuuity.security.server;

import com.google.inject.Inject;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.util.security.Constraint;

import javax.security.auth.login.Configuration;

/**
 *
 */
public abstract class JAASAuthenticationHandler extends ConstraintSecurityHandler {

  @Inject
  public JAASAuthenticationHandler(String loginModuleName) throws Exception {
    super();

    Constraint constraint = new Constraint();
    constraint.setRoles(new String[] {"*"});
    constraint.setAuthenticate(true);

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    JAASLoginService jaasLoginService = new JAASLoginService();
    jaasLoginService.setLoginModuleName(loginModuleName);
    jaasLoginService.setConfiguration(getConfiguration());

    this.setStrict(false);
    this.setIdentityService(new DefaultIdentityService());
    this.setAuthenticator(new BasicAuthenticator());
    this.setLoginService(jaasLoginService);
    this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
  }

  protected abstract Configuration getConfiguration();
}
