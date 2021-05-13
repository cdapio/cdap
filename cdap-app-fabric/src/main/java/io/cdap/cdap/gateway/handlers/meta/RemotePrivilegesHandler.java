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
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.AuthorizationPrivilege;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.PermissionAdapterFactory;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Privilege;
import io.cdap.cdap.proto.security.VisibilityRequest;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private static final Type SET_OF_ACTIONS = new TypeLiteral<Set<Action>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .registerTypeAdapterFactory(new PermissionAdapterFactory())
    .create();

  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AccessEnforcer accessEnforcer;

  @Inject
  RemotePrivilegesHandler(PrivilegesManager privilegesManager, AuthorizationEnforcer authorizationEnforcer,
                          AccessEnforcer accessEnforcer) {
    this.privilegesManager = privilegesManager;
    this.authorizationEnforcer = authorizationEnforcer;
    this.accessEnforcer = accessEnforcer;
  }

  @POST
  @Path("/enforce")
  public void enforce(FullHttpRequest request, HttpResponder responder) throws Exception {
    AuthorizationPrivilege authorizationPrivilege = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                                  AuthorizationPrivilege.class);
    LOG.debug("Enforcing for {}", authorizationPrivilege);
    Set<Permission> permissions = authorizationPrivilege.getPermissions();
    if (authorizationPrivilege.getActions() != null) {
      permissions = Stream.of(permissions, authorizationPrivilege.getActions())
        .flatMap(set -> set.stream())
        .collect(Collectors.toSet());
    }
    accessEnforcer.enforce(authorizationPrivilege.getEntity(), authorizationPrivilege.getPrincipal(),
                                  permissions);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/isSingleVisible")
  public void isSingleVisible(FullHttpRequest request, HttpResponder responder) throws Exception {
    AuthorizationPrivilege authorizationPrivilege = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                                  AuthorizationPrivilege.class);
    LOG.debug("Enforcing visibility for {}", authorizationPrivilege);
    authorizationEnforcer.isVisible(authorizationPrivilege.getEntity(), authorizationPrivilege.getPrincipal());
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
    Set<? extends EntityId> visiableEntities = authorizationEnforcer.isVisible(entityIds, principal);
    LOG.debug("Returning entities visible for principal {} as {}", principal, visiableEntities);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(visiableEntities));
  }

  @POST
  @Path("/listPrivileges")
  public void listPrivileges(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    Principal principal = deserializeNext(arguments);
    LOG.trace("Listing privileges for principal {}", principal);
    Set<Privilege> privileges = privilegesManager.listPrivileges(principal);
    LOG.debug("Returning privileges for principal {} as {}", principal, privileges);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(privileges));
  }

  @POST
  @Path("/grant")
  public void grant(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    Principal principal = deserializeNext(arguments);
    Set<Action> actions = deserializeNext(arguments, SET_OF_ACTIONS);
    LOG.trace("Granting {} on {} to {}", actions, entityId, principal);
    privilegesManager.grant(Authorizable.fromEntityId(entityId), principal, actions);
    LOG.info("Granted {} on {} to {} successfully", actions, entityId, principal);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/revoke")
  public void revoke(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    Principal principal = deserializeNext(arguments);
    Set<Action> actions = deserializeNext(arguments, SET_OF_ACTIONS);
    LOG.trace("Revoking {} on {} from {}", actions, entityId, principal);
    privilegesManager.revoke(Authorizable.fromEntityId(entityId), principal, actions);
    LOG.info("Revoked {} on {} from {} successfully", actions, entityId, principal);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/revokeAll")
  public void revokeAll(FullHttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    LOG.trace("Revoking all actions on {}", entityId);
    privilegesManager.revoke(Authorizable.fromEntityId(entityId));
    LOG.info("Revoked all actions on {} successfully", entityId);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
