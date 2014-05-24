package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Inject;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Request;

import java.io.IOException;
import java.net.URL;
import javax.security.auth.login.Configuration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handler for basic authentication of users.
 */
@Path("/")
public class BasicAuthenticationHandler extends AbstractAuthenticationHandler {
  private IdentityService identityService;

  @Inject
  public BasicAuthenticationHandler(CConfiguration configuration) throws Exception {
    super(configuration);
  }

  @Path("token")
  @GET
  @Override
  public void handle(String pathInContext, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException {

    super.handle(pathInContext, baseRequest, request, response);
  }

  @Override
  protected LoginService getHandlerLoginService() {
//    String realmFile = configuration.get(Constants.Security.BASIC_REALM_FILE);
    URL realmFile = getClass().getResource("/realm.properties");
    HashLoginService loginService = new HashLoginService();
    loginService.setConfig(realmFile.toExternalForm());
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
