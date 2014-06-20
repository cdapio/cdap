package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.inject.Inject;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.security.Constraint;

import java.io.IOException;
import javax.security.auth.login.Configuration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

/**
 * An abstract authentication handler that provides basic functionality including
 * setting of constraints and setting of different required services.
 */
@Path("/")
public abstract class AbstractAuthenticationHandler extends ConstraintSecurityHandler {
  protected final CConfiguration configuration;

  @Inject
  public AbstractAuthenticationHandler(CConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Authentication Handler paths.
   */
  public static final class Paths {
    public static final String GET_TOKEN = "/token";
    public static final String GET_EXTENDED_TOKEN = "extendedToken";
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

  @Path(Paths.GET_TOKEN)
  @GET
  public String token(@Context HttpServletRequest request, @Context HttpServletResponse response)
    throws IOException, ServletException {

    super.handle(Paths.GET_TOKEN, Request.getRequest(request), request, response);
    return "";
  }

  @Path(Paths.GET_EXTENDED_TOKEN)
  @GET
  public String extendedToken(@Context HttpServletRequest request, @Context HttpServletResponse response)
    throws IOException, ServletException {

    super.handle(Paths.GET_EXTENDED_TOKEN, Request.getRequest(request), request, response);
    return "";
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
