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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.common.logging.AuditLogEntry;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.AuthorizationRequest;
import co.cask.cdap.proto.security.GrantRequest;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.RevokeRequest;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.authorization.AuthorizerInstantiatorService;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.http.HttpResponder;
import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Exposes {@link Authorizer} operations via HTTP.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/security/authorization")
public class AuthorizationHandler extends AbstractAppFabricHttpHandler {

  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("authorization-access");
  private final AuthorizerInstantiatorService authorizerInstantiatorService;
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private static final Type PRIVILEGE_SET_TYPE = new TypeToken<Set<Privilege>>() { }.getType();
  private final boolean authenticationEnabled;
  private final boolean authorizationEnabled;
  private final EntityExistenceVerifier entityExistenceVerifier;

  @Inject
  AuthorizationHandler(AuthorizerInstantiatorService authorizerInstantiatorService, CConfiguration cConf,
                       EntityExistenceVerifier entityExistenceVerifier) {
    this.authorizerInstantiatorService = authorizerInstantiatorService;
    this.authenticationEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.entityExistenceVerifier = entityExistenceVerifier;
  }

  @Path("/privileges/grant")
  @POST
  public void grant(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    ensureSecurityEnabled();

    GrantRequest request = parseBody(httpRequest, GrantRequest.class);
    verifyAuthRequest(request);

    Set<Action> actions = request.getActions() == null ? EnumSet.allOf(Action.class) : request.getActions();
    // enforce that the user granting access has admin privileges on the entity
    authorizerInstantiatorService.get().enforce(request.getEntity(), SecurityRequestContext.toPrincipal(),
                                                Action.ADMIN);
    authorizerInstantiatorService.get().grant(request.getEntity(), request.getPrincipal(), actions);

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

  @Path("/privileges/revoke")
  @POST
  public void revoke(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    ensureSecurityEnabled();

    RevokeRequest request = parseBody(httpRequest, RevokeRequest.class);
    verifyAuthRequest(request);

    // enforce that the user revoking access has admin privileges on the entity
    authorizerInstantiatorService.get().enforce(request.getEntity(), SecurityRequestContext.toPrincipal(),
                                                Action.ADMIN);
    if (request.getPrincipal() == null && request.getActions() == null) {
      authorizerInstantiatorService.get().revoke(request.getEntity());
    } else {
      Set<Action> actions = request.getActions() == null ? EnumSet.allOf(Action.class) : request.getActions();
      authorizerInstantiatorService.get().revoke(request.getEntity(), request.getPrincipal(), actions);
    }

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, request, HttpResponseStatus.OK);
  }

  @Path("{principal-type}/{principal-name}/privileges")
  @GET
  public void listPrivileges(HttpRequest httpRequest, HttpResponder httpResponder,
                             @PathParam("principal-type") String principalType,
                             @PathParam("principal-name") String principalName) throws Exception {
    ensureSecurityEnabled();
    Principal principal = new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
    httpResponder.sendJson(HttpResponseStatus.OK, authorizerInstantiatorService.get().listPrivileges(principal),
                           PRIVILEGE_SET_TYPE, GSON);
    createLogEntry(httpRequest, null, HttpResponseStatus.OK);
  }


  /********************************************************************************************************************
   * Role Management : For Role Based Access Control
   ********************************************************************************************************************/

  @Path("/roles/{role-name}")
  @PUT
  public void createRole(HttpRequest httpRequest, HttpResponder httpResponder,
                         @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    authorizerInstantiatorService.get().createRole(new Role(roleName));
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, null, HttpResponseStatus.OK);
  }

  @Path("/roles/{role-name}")
  @DELETE
  public void dropRole(HttpRequest httpRequest, HttpResponder httpResponder,
                       @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    authorizerInstantiatorService.get().dropRole(new Role(roleName));
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, null, HttpResponseStatus.OK);
  }

  @Path("/roles")
  @GET
  public void listAllRoles(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    ensureSecurityEnabled();
    httpResponder.sendJson(HttpResponseStatus.OK, authorizerInstantiatorService.get().listAllRoles());
    createLogEntry(httpRequest, null, HttpResponseStatus.OK);
  }

  @Path("{principal-type}/{principal-name}/roles")
  @GET
  public void listRoles(HttpRequest httpRequest, HttpResponder httpResponder,
                        @PathParam("principal-type") String principalType,
                        @PathParam("principal-name") String principalName) throws Exception {
    ensureSecurityEnabled();
    Principal principal = new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
    httpResponder.sendJson(HttpResponseStatus.OK, authorizerInstantiatorService.get().listRoles(principal));
    createLogEntry(httpRequest, null, HttpResponseStatus.OK);
  }

  @Path("/{principal-type}/{principal-name}/roles/{role-name}")
  @PUT
  public void addRoleToPrincipal(HttpRequest httpRequest, HttpResponder httpResponder,
                                 @PathParam("principal-type") String principalType,
                                 @PathParam("principal-name") String principalName,
                                 @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    authorizerInstantiatorService.get().addRoleToPrincipal(new Role(roleName),
                                  new Principal(principalName,
                                                Principal.PrincipalType.valueOf(principalType.toUpperCase())));
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, null, HttpResponseStatus.OK);
  }

  @Path("/{principal-type}/{principal-name}/roles/{role-name}")
  @DELETE
  public void removeRoleFromPrincipal(HttpRequest httpRequest, HttpResponder httpResponder,
                                      @PathParam("principal-type") String principalType,
                                      @PathParam("principal-name") String principalName,
                                      @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    authorizerInstantiatorService.get().removeRoleFromPrincipal(new Role(roleName),
                                       new Principal(principalName,
                                                     Principal.PrincipalType.valueOf(principalType.toUpperCase())));
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, null, HttpResponseStatus.OK);
  }

  private void ensureSecurityEnabled() throws FeatureDisabledException {
    if (!authenticationEnabled) {
      throw new FeatureDisabledException(FeatureDisabledException.Feature.AUTHENTICATION,
                                         FeatureDisabledException.CDAP_SITE, Constants.Security.ENABLED, "true");
    }
    if (!authorizationEnabled) {
      throw new FeatureDisabledException(FeatureDisabledException.Feature.AUTHORIZATION,
                                         FeatureDisabledException.CDAP_SITE, Constants.Security.Authorization.ENABLED,
                                         "true");
    }
  }

  private void verifyAuthRequest(AuthorizationRequest request) throws BadRequestException, NotFoundException {
    if (request == null) {
      throw new BadRequestException("Missing request body");
    }
    EntityId entity = request.getEntity();
    entityExistenceVerifier.ensureExists(entity);
  }

  private void createLogEntry(HttpRequest httpRequest, @Nullable AuthorizationRequest request,
                              HttpResponseStatus responseStatus) throws UnknownHostException {
    AuditLogEntry logEntry = new AuditLogEntry();
    logEntry.setUserName(Objects.firstNonNull(SecurityRequestContext.getUserId(), "-"));
    logEntry.setClientIP(InetAddress.getByName(Objects.firstNonNull(SecurityRequestContext.getUserIP(), "0.0.0.0")));
    logEntry.setRequestLine(httpRequest.getMethod(), httpRequest.getUri(), httpRequest.getProtocolVersion());
    if (request != null) {
      logEntry.setRequestBody(String.format("[%s %s %s]", request.getPrincipal(), request.getEntity(),
                                            request.getActions()));
    }
    logEntry.setResponseCode(responseStatus.getCode());
    AUDIT_LOG.trace(logEntry.toString());
  }
}
