/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers.meta;

import co.cask.cdap.common.internal.remote.MethodArgument;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.AuthorizationPrivilege;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.VisibilityRequest;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.TypeLiteral;
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
  private static final Type SET_OF_ACTIONS = new TypeLiteral<Set<Action>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;

  @Inject
  RemotePrivilegesHandler(PrivilegesManager privilegesManager, AuthorizationEnforcer authorizationEnforcer) {
    this.privilegesManager = privilegesManager;
    this.authorizationEnforcer = authorizationEnforcer;
  }

  @POST
  @Path("/enforce")
  public void enforce(FullHttpRequest request, HttpResponder responder) throws Exception {
    AuthorizationPrivilege authorizationPrivilege = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                                  AuthorizationPrivilege.class);
    LOG.debug("Enforcing for {}", authorizationPrivilege);
    authorizationEnforcer.enforce(authorizationPrivilege.getEntity(), authorizationPrivilege.getPrincipal(),
                                  authorizationPrivilege.getAction());
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
