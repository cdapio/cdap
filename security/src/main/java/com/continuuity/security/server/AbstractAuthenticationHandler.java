package com.continuuity.security.server;

import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.util.security.Constraint;

import javax.security.auth.login.Configuration;

/**
 *
 */
public abstract class AbstractAuthenticationHandler extends ConstraintSecurityHandler {

  public AbstractAuthenticationHandler() {
    Constraint constraint = new Constraint();
    constraint.setRoles(new String[] {"*"});
    constraint.setAuthenticate(true);

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
    this.setStrict(false);
    this.setIdentityService(getHandlerIdentityService());
    this.setAuthenticator(getHandlerAuthenticator());
    this.setLoginService(getHandlerLoginService());
  }

  protected abstract LoginService getHandlerLoginService();

  protected abstract Authenticator getHandlerAuthenticator();

  protected abstract IdentityService getHandlerIdentityService();

  protected abstract Configuration getLoginModuleConfiguration();
}
