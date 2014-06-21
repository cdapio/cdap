package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.inject.Inject;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.util.security.Constraint;

import javax.security.auth.login.Configuration;
import javax.ws.rs.Path;

/**
 * An abstract authentication handler that provides basic functionality including
 * setting of constraints and setting of different required services.
 */
@Path("/*")
public abstract class AbstractAuthenticationHandler extends ConstraintSecurityHandler {
  protected final CConfiguration configuration;

  @Inject
  public AbstractAuthenticationHandler(CConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Initialize the handler context and other related services.
   */
  public void init() throws Exception {
    Constraint constraint = new Constraint();
    constraint.setRoles(new String[]{"*"});
    constraint.setAuthenticate(true);

    if (configuration.getBoolean(Constants.Security.SSL_ENABLED)) {
      constraint.setDataConstraint(Constraint.DC_CONFIDENTIAL);
    }

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
    this.setStrict(false);
    this.setIdentityService(getHandlerIdentityService());
    this.setAuthenticator(getHandlerAuthenticator());
    this.setLoginService(getHandlerLoginService());
    this.doStart();
  }

  /**
   * Get a {@link org.eclipse.jetty.security.LoginService} for the handler.
   * @return
   */
  protected abstract LoginService getHandlerLoginService();

  /**
   * Get an {@link org.eclipse.jetty.security.Authenticator} for the handler.
   * @return
   */
  protected abstract Authenticator getHandlerAuthenticator();

  /**
   * Get an {@link org.eclipse.jetty.security.IdentityService} for the handler.
   * @return
   */
  protected abstract IdentityService getHandlerIdentityService();

  /**
   * Get configuration for the LoginModule.
   * @return
   */
  protected abstract Configuration getLoginModuleConfiguration();
}
