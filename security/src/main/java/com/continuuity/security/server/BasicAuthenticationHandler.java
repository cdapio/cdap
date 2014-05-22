package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.inject.Inject;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;

import javax.security.auth.login.Configuration;

/**
 * Handler for basic authentication of users.
 */
public class BasicAuthenticationHandler extends AbstractAuthenticationHandler {
  private IdentityService identityService;

  @Inject
  public BasicAuthenticationHandler(CConfiguration configuration) throws Exception {
    super(configuration);
  }

  @Override
  protected LoginService getHandlerLoginService() {
    String realmFile = configuration.get(Constants.Security.BASIC_REALM_FILE);
    HashLoginService loginService = new HashLoginService();
    loginService.setConfig(realmFile);
    loginService.setIdentityService(getHandlerIdentityService());
    return loginService;
  }

  @Override
  protected Authenticator getHandlerAuthenticator() {
    return new BasicAuthenticator();
  }

  @Override
  protected IdentityService getHandlerIdentityService() {
    if (identityService == null) {
      identityService = new DefaultIdentityService();
    }
    return identityService;
  }

  @Override
  protected Configuration getLoginModuleConfiguration() {
    return null;
  }
}
