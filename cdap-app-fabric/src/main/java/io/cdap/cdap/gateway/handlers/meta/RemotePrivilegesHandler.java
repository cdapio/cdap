/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.meta;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.internal.remote.MethodArgument;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.AuthorizationPrivilege;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.PermissionAdapterFactory;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.security.VisibilityRequest;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * An HTTP Handler that runs inside the master and communicates directly with an authorization backend to list and
 * manage privileges.
 */
@Path(AbstractRemoteSystemOpsHandler.VERSION + "/execute")
public class RemotePrivilegesHandler extends AbstractRemoteSystemOpsHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RemotePrivilegesHandler.class);
  private static final Type SET_OF_PERMISSIONS = new TypeLiteral<Set<? extends Permission>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .registerTypeAdapterFactory(new PermissionAdapterFactory())
    .create();

  private final PermissionManager permissionManager;
  private final AccessEnforcer accessEnforcer;

  @Inject
  RemotePrivilegesHandler(PermissionManager permissionManager, AccessEnforcer accessEnforcer) {
    this.permissionManager = permissionManager;
    this.accessEnforcer = accessEnforcer;
  }

  @POST
  @Path("/enforce")
  public void enforce(FullHttpRequest request, HttpResponder responder) throws Exception {
    AuthorizationPrivilege authorizationPrivilege = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                                  AuthorizationPrivilege.class);
    LOG.debug("Enforcing for {}", authorizationPrivilege);
    Set<Permission> permissions = authorizationPrivilege.getPermissions();
    if (authorizationPrivilege.getChildEntityType() != null) {
      //It's expected that we'll always have one, but let's handle generic case
      for (Permission permission: permissions) {
        accessEnforcer.enforceOnParent(authorizationPrivilege.getChildEntityType(), authorizationPrivilege.getEntity(),
                                       authorizationPrivilege.getPrincipal(), permission);
      }
    } else {
      accessEnforcer.enforce(authorizationPrivilege.getEntity(), authorizationPrivilege.getPrincipal(),
                             permissions);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/isVisible")
  public void isVisible(FullHttpRequest request, HttpResponder responder) throws Exception {
    VisibilityRequest visibilityRequest = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                        VisibilityRequest.class);
    Principal principal = visibilityRequest.getPrincipal();
    Set<EntityId> entityIds = visibilityRequest.getEntityIds();
    LOG.trace("Checking visibility for principal {} on entities {}", principal, entityIds);
    Set<? extends EntityId> visiableEntities = accessEnforcer.isVisible(entityIds, principal);
    LOG.debug("Returning entities visible for principal {} as {}", principal, visiableEntities);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(visiableEntities));
  }

  @POST
  @Path("/listPrivileges")
  public void listPrivileges(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    Principal principal = deserializeNext(arguments);
    LOG.trace("Listing grantedPermissions for principal {}", principal);
    Set<GrantedPermission> grantedPermissions = permissionManager.listGrants(principal);
    LOG.debug("Returning grantedPermissions for principal {} as {}", principal, grantedPermissions);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(grantedPermissions));
  }

  @POST
  @Path("/grant")
  public void grant(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    Principal principal = deserializeNext(arguments);
    Set<? extends Permission> permissions = deserializeNext(arguments, SET_OF_PERMISSIONS);
    LOG.trace("Granting {} on {} to {}", permissions, entityId, principal);
    permissionManager.grant(Authorizable.fromEntityId(entityId), principal, permissions);
    LOG.info("Granted {} on {} to {} successfully", permissions, entityId, principal);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/revoke")
  public void revoke(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    Principal principal = deserializeNext(arguments);
    Set<? extends Permission> permissions = deserializeNext(arguments, SET_OF_PERMISSIONS);
    LOG.trace("Revoking {} on {} from {}", permissions, entityId, principal);
    permissionManager.revoke(Authorizable.fromEntityId(entityId), principal, permissions);
    LOG.info("Revoked {} on {} from {} successfully", permissions, entityId, principal);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/revokeAll")
  public void revokeAll(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    LOG.trace("Revoking all actions on {}", entityId);
    permissionManager.revoke(Authorizable.fromEntityId(entityId));
    LOG.info("Revoked all actions on {} successfully", entityId);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
