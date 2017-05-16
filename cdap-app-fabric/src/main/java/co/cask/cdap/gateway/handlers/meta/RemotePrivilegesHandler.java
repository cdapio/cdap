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
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.http.HttpResponder;
import com.google.inject.TypeLiteral;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
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

  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;

  @Inject
  RemotePrivilegesHandler(PrivilegesManager privilegesManager, AuthorizationEnforcer authorizationEnforcer) {
    this.privilegesManager = privilegesManager;
    this.authorizationEnforcer = authorizationEnforcer;
  }

  @POST
  @Path("/enforce")
  public void enforce(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    Principal principal = deserializeNext(arguments);
    Action action = deserializeNext(arguments);
    LOG.info("Enforcing {} on {} for {}", action, entityId, principal);
    authorizationEnforcer.enforce(entityId, principal, action);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/listPrivileges")
  public void listPrivileges(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    Principal principal = deserializeNext(arguments);
    LOG.trace("Listing privileges for principal {}", principal);
    Set<Privilege> privileges = privilegesManager.listPrivileges(principal);
    LOG.debug("Returning privileges for principal {} as {}", principal, privileges);
    responder.sendJson(HttpResponseStatus.OK, privileges);
  }

  @POST
  @Path("/grant")
  public void grant(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    Principal principal = deserializeNext(arguments);
    Set<Action> actions = deserializeNext(arguments, SET_OF_ACTIONS);
    LOG.debug("Granting {} on {} to {}", actions, entityId, principal);
    privilegesManager.grant(entityId, principal, actions);
    LOG.info("Granted {} on {} to {} successfully", actions, entityId, principal);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/revoke")
  public void revoke(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    Principal principal = deserializeNext(arguments);
    Set<Action> actions = deserializeNext(arguments, SET_OF_ACTIONS);
    LOG.trace("Revoking {} on {} from {}", actions, entityId, principal);
    privilegesManager.revoke(entityId, principal, actions);
    LOG.info("Revoked {} on {} from {} successfully", actions, entityId, principal);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/revokeAll")
  public void revokeAll(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    EntityId entityId = deserializeNext(arguments);
    LOG.trace("Revoking all actions on {}", entityId);
    privilegesManager.revoke(entityId);
    LOG.info("Revoked all actions on {} successfully", entityId);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
