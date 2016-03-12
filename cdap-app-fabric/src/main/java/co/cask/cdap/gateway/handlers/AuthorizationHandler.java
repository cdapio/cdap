/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.SecurityRequestContext;
import co.cask.cdap.common.logging.AuditLogEntry;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.AuthorizationRequest;
import co.cask.cdap.proto.security.CheckAuthorizedRequest;
import co.cask.cdap.proto.security.GrantRequest;
import co.cask.cdap.proto.security.RevokeRequest;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.http.HttpResponder;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Exposes {@link Authorizer} operations via HTTP.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/security")
public class AuthorizationHandler extends AbstractAppFabricHttpHandler {

  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("authorization-access");
  private final Authorizer authorizer;
  private final boolean enabled;

  @Inject
  AuthorizationHandler(Authorizer authorizer, CConfiguration conf) {
    this.authorizer = authorizer;
    this.enabled = conf.getBoolean(Constants.Security.Authorization.ENABLED);
  }

  private void createLogEntry(HttpRequest httpRequest, AuthorizationRequest request,
                              HttpResponseStatus responseStatus) throws UnknownHostException {
    String reqBody = String.format("[%s %s %s]", request.getPrincipal(), request.getEntity(), request.getActions());
    AuditLogEntry logEntry = new AuditLogEntry();
    logEntry.setUserName(SecurityRequestContext.getUserId().or("-"));
    logEntry.setClientIP(InetAddress.getByName(SecurityRequestContext.getUserIP().or("0.0.0.0")));
    logEntry.setRequestLine(httpRequest.getMethod(), httpRequest.getUri(), httpRequest.getProtocolVersion());
    logEntry.setRequestBody(reqBody);
    logEntry.setResponseCode(responseStatus.getCode());
    AUDIT_LOG.trace(logEntry.toString());
  }

  @Path("/authorized")
  @POST
  public void authorized(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    ensureAuthorizationEnabled();

    CheckAuthorizedRequest request = parseBody(httpRequest, CheckAuthorizedRequest.class);
    verifyAuthRequest(request);

    Set<Action> actions = Optional.fromNullable(request.getActions()).or(Collections.<Action>emptySet());
    if (actions.isEmpty()) {
      httpResponder.sendString(HttpResponseStatus.OK, "No actions to check for authorization in request");
      return;
    }

    for (Action action : request.getActions()) {
      authorizer.enforce(request.getEntity(), request.getPrincipal(), action);
    }
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

  @Path("/grant")
  @POST
  public void grant(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    ensureAuthorizationEnabled();

    GrantRequest request = parseBody(httpRequest, GrantRequest.class);
    verifyAuthRequest(request);

    Set<Action> actions = request.getActions() == null ? EnumSet.allOf(Action.class) : request.getActions();
    authorizer.grant(request.getEntity(), request.getPrincipal(), actions);

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

  @Path("/revoke")
  @POST
  public void revoke(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    ensureAuthorizationEnabled();

    RevokeRequest request = parseBody(httpRequest, RevokeRequest.class);
    verifyAuthRequest(request);

    if (request.getPrincipal() == null && request.getActions() == null) {
      authorizer.revoke(request.getEntity());
    } else {
      Set<Action> actions = request.getActions() == null ? EnumSet.allOf(Action.class) : request.getActions();
      authorizer.revoke(request.getEntity(), request.getPrincipal(), actions);
    }

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

  private void ensureAuthorizationEnabled() throws FeatureDisabledException {
    if (!enabled) {
      throw new FeatureDisabledException("Authorization", "cdap-site.xml", Constants.Security.Authorization.ENABLED,
                                         "true");
    }
  }

  private void verifyAuthRequest(AuthorizationRequest request) throws BadRequestException {
    if (request == null) {
      throw new BadRequestException("Missing request body");
    }
  }
}
