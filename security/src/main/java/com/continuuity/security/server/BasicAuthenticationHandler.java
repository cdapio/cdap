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
import org.eclipse.jetty.server.Request;

import java.io.IOException;
import javax.security.auth.login.Configuration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

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

  @Path("ping")
  @GET
  @Produces("application/json")
  public Response ok() {
    return Response.ok("OK").build();
  }

  @Path("token")
  @GET
  public Void token(@Context HttpServletRequest request, @Context HttpServletResponse response) throws IOException, ServletException {
    super.handle("/token", Request.getRequest(request), request, response);
    return null;
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
