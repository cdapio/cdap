/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security.server;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.inject.Inject;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.security.Constraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.security.auth.login.Configuration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;

/**
 * An abstract authentication handler that provides basic functionality including
 * setting of constraints and setting of different required services.
 */
@Path("/*")
public abstract class AbstractAuthenticationHandler extends ConstraintSecurityHandler {
  private static final Logger AUTHENTICATION_AUDIT_LOG = LoggerFactory.getLogger("auth");
  private final DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

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

  @Override
  public void handle(String pathInContext, Request baseRequest, HttpServletRequest request,
                     HttpServletResponse response) throws IOException, ServletException {
    try {
      super.handle(pathInContext, baseRequest, request, response);
    } finally {
      AuthenticationLogEntry logEntry = new AuthenticationLogEntry(
        request.getAuthType(), request.getRemoteUser(), request.getRemoteAddr(), response);
      AUTHENTICATION_AUDIT_LOG.trace(logEntry.toString());
    }
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

  private final class AuthenticationLogEntry {

    /** Each audit log field will default to "-" if the field is missing or not supported. */
    private static final String DEFAULT_VALUE = "-";

    private final String authType;
    private final String userName;
    private final boolean success;
    private final Date date;
    private final String remoteAddr;

    public AuthenticationLogEntry(String authType, String userName, String remoteAddr, HttpServletResponse response) {
      this.authType = authType;
      this.userName = userName;
      this.remoteAddr = remoteAddr;
      this.success = response.getStatus() == HttpServletResponse.SC_OK;
      this.date = new Date();
    }

    @Override
    public String toString() {
      return String.format("%s %s [%s] %s %b",
                           fieldOrDefault(remoteAddr),
                           fieldOrDefault(userName),
                           dateFormat.format(date),
                           fieldOrDefault(authType),
                           success);
    }

    private String fieldOrDefault(Object field) {
      return field == null ? DEFAULT_VALUE : field.toString();
    }
  }
}
