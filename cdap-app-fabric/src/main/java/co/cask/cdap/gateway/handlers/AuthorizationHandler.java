/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.SecurityRequestContext;
import co.cask.cdap.common.logging.AuditLogEntry;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.security.AuthorizationRequest;
import co.cask.cdap.proto.security.CheckAuthorizedRequest;
import co.cask.cdap.proto.security.CheckAuthorizedResponse;
import co.cask.cdap.proto.security.GrantRequest;
import co.cask.cdap.proto.security.RevokeRequest;
import co.cask.cdap.security.authorization.AuthorizationPlugin;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Exposes {@link AuthorizationPlugin} operations via HTTP.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/security")
public class AuthorizationHandler extends AbstractAppFabricHttpHandler {

  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("authorization-access");
  private final AuthorizationPlugin auth;
  private final boolean enabled;

  @Inject
  public AuthorizationHandler(AuthorizationPlugin auth, CConfiguration conf) {
    this.auth = auth;
    this.enabled = conf.getBoolean(Constants.Security.Authorization.ENABLED);
  }

  private void createLogEntry(HttpRequest httpRequest, AuthorizationRequest request,
                              HttpResponseStatus responseStatus) throws UnknownHostException {
    String reqBody = String.format("[%s %s %s]", request.getUser(), request.getEntity(), request.getActions());
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
    if (!enabled) {
      httpResponder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    CheckAuthorizedRequest request = parseBody(httpRequest, CheckAuthorizedRequest.class);
    if (request == null) {
      throw new BadRequestException("Missing request body");
    }

    CheckAuthorizedResponse response = new CheckAuthorizedResponse(
      auth.authorized(request.getEntity(), request.getUser(), request.getActions()));
    httpResponder.sendJson(HttpResponseStatus.OK, response);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

  @Path("/grant")
  @POST
  public void grant(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    if (!enabled) {
      httpResponder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    GrantRequest request = parseBody(httpRequest, GrantRequest.class);
    if (request == null) {
      throw new BadRequestException("Missing request body");
    }

    if (request.getActions() == null) {
      auth.grant(request.getEntity(), request.getUser());
    } else {
      auth.grant(request.getEntity(), request.getUser(), request.getActions());
    }

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

  @Path("/revoke")
  @POST
  public void revoke(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    if (!enabled) {
      httpResponder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    RevokeRequest request = parseBody(httpRequest, RevokeRequest.class);
    if (request == null) {
      throw new BadRequestException("Missing request body");
    }

    if (request.getUser() == null && request.getActions() == null) {
      auth.revoke(request.getEntity());
    } else if (request.getActions() == null) {
      auth.revoke(request.getEntity(), request.getUser());
    } else {
      auth.revoke(request.getEntity(), request.getUser(), request.getActions());
    }

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

}
