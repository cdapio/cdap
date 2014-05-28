package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;

/**
 * An abstract authentication handler that supports the JAAS interface for external authentication.
 */
public abstract class JAASAuthenticationHandler extends AbstractAuthenticationHandler {

  public JAASAuthenticationHandler(CConfiguration configuration) {
    super(configuration);
  }

  @Override
  public IdentityService getHandlerIdentityService() {
    return new DefaultIdentityService();
  }

  @Override
  protected Authenticator getHandlerAuthenticator() {
    return new BasicAuthenticator();
  }

  @Override
  protected LoginService getHandlerLoginService() {
    JAASLoginService jaasLoginService = new JAASLoginService();
    jaasLoginService.setLoginModuleName("jaasLoginService");
    jaasLoginService.setConfiguration(getLoginModuleConfiguration());
    return jaasLoginService;
  }
}
