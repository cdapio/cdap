/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.FeatureDisabledException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.AuditLogEntry;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.AuthorizationRequest;
import io.cdap.cdap.proto.security.GrantRequest;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.PermissionAdapterFactory;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Privilege;
import io.cdap.cdap.proto.security.RevokeRequest;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Exposes {@link AccessController} operations via HTTP.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/security/authorization")
public class AuthorizationHandler extends AbstractAppFabricHttpHandler {

  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("authorization-access");
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .registerTypeAdapterFactory(new PermissionAdapterFactory())
    .create();
  private static final Type PRIVILEGE_SET_TYPE = new TypeToken<Set<Privilege>>() { }.getType();

  private final boolean authenticationEnabled;
  private final boolean authorizationEnabled;
  private final PermissionManager permissionManager;
  private final AccessController accessController;
  private final AuthenticationContext authenticationContext;

  @Inject
  AuthorizationHandler(PermissionManager permissionManager, AccessControllerInstantiator accessControllerInstantiator,
                       CConfiguration cConf, AuthenticationContext authenticationContext) {
    this.permissionManager = permissionManager;
    this.accessController = accessControllerInstantiator.get();
    this.authenticationContext = authenticationContext;
    this.authenticationEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
  }

  @Path("/privileges/grant")
  @POST
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void grant(FullHttpRequest httpRequest, HttpResponder httpResponder) throws BadRequestException,
    FeatureDisabledException, UnknownHostException, AccessException {
    ensureSecurityEnabled();

    GrantRequest request = parseBody(httpRequest, GrantRequest.class);
    if (request == null) {
      throw new BadRequestException("Missing request body");
    }

    permissionManager.grant(request.getAuthorizable(), request.getPrincipal(), getRequestPermissions(request));

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }

  @Path("/privileges/revoke")
  @POST
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void revoke(FullHttpRequest httpRequest, HttpResponder httpResponder) throws FeatureDisabledException,
    BadRequestException, UnknownHostException, AccessException {
    ensureSecurityEnabled();

    RevokeRequest request = parseBody(httpRequest, RevokeRequest.class);
    if (request == null) {
      throw new BadRequestException("Missing request body");
    }

    if (request.getPrincipal() == null && request.getActions() == null && request.getPermissions() == null) {
      permissionManager.revoke(request.getAuthorizable());
    } else {
      permissionManager.revoke(request.getAuthorizable(), request.getPrincipal(), getRequestPermissions(request));
    }

    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }

  @Path("{principal-type}/{principal-name}/privileges")
  @GET
  public void listPrivileges(HttpRequest httpRequest, HttpResponder httpResponder,
                             @PathParam("principal-type") String principalType,
                             @PathParam("principal-name") String principalName) throws Exception {
    ensureSecurityEnabled();
    Principal principal = new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
    httpResponder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(accessController.listGrants(principal), PRIVILEGE_SET_TYPE));
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }


  /********************************************************************************************************************
   * Role Management : For Role Based Access Control
   ********************************************************************************************************************/

  @Path("/roles/{role-name}")
  @PUT
  public void createRole(HttpRequest httpRequest, HttpResponder httpResponder,
                         @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    accessController.createRole(new Role(roleName));
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }

  @Path("/roles/{role-name}")
  @DELETE
  public void dropRole(HttpRequest httpRequest, HttpResponder httpResponder,
                       @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    accessController.dropRole(new Role(roleName));
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }

  @Path("/roles")
  @GET
  public void listAllRoles(HttpRequest httpRequest, HttpResponder httpResponder) throws Exception {
    ensureSecurityEnabled();
    httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(accessController.listAllRoles()));
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }

  @Path("{principal-type}/{principal-name}/roles")
  @GET
  public void listRoles(HttpRequest httpRequest, HttpResponder httpResponder,
                        @PathParam("principal-type") String principalType,
                        @PathParam("principal-name") String principalName) throws Exception {
    ensureSecurityEnabled();
    Principal principal = new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
    httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(accessController.listRoles(principal)));
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }

  @Path("/{principal-type}/{principal-name}/roles/{role-name}")
  @PUT
  public void addRoleToPrincipal(HttpRequest httpRequest, HttpResponder httpResponder,
                                 @PathParam("principal-type") String principalType,
                                 @PathParam("principal-name") String principalName,
                                 @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    Principal principal = new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
    accessController.addRoleToPrincipal(new Role(roleName), principal);
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, HttpResponseStatus.OK);
  }

  @Path("/{principal-type}/{principal-name}/roles/{role-name}")
  @DELETE
  public void removeRoleFromPrincipal(HttpRequest httpRequest, HttpResponder httpResponder,
                                      @PathParam("principal-type") String principalType,
                                      @PathParam("principal-name") String principalName,
                                      @PathParam("role-name") String roleName) throws Exception {
    ensureSecurityEnabled();
    Principal principal = new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
    accessController.removeRoleFromPrincipal(new Role(roleName), principal);
    httpResponder.sendStatus(HttpResponseStatus.OK);
    createLogEntry(httpRequest, HttpResponseStatus.OK);
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

  private void createLogEntry(HttpRequest httpRequest, HttpResponseStatus responseStatus) throws UnknownHostException {
    InetAddress clientAddr = InetAddress.getByName(Objects.firstNonNull(SecurityRequestContext.getUserIP(), "0.0.0.0"));
    AuditLogEntry logEntry = new AuditLogEntry(httpRequest, clientAddr.getHostAddress());
    logEntry.setUserName(authenticationContext.getPrincipal().getName());
    logEntry.setResponse(responseStatus.code(), 0L);
    AUDIT_LOG.trace(logEntry.toString());
  }

  private Set<? extends Permission> getRequestPermissions(AuthorizationRequest request) {
    Set<? extends Permission> permissions = Objects.firstNonNull(request.getPermissions(), Collections.emptySet());
    if (request.getActions() != null) {
      permissions = Stream.concat(permissions.stream(), request.getActions().stream().map(Action::getPermission))
        .collect(Collectors.toSet());
    }
    return permissions;
  }
}
