/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.dataset.lib.ACLTable;
import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.EntityType;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;
import co.cask.cdap.api.security.PrincipalType;
import co.cask.cdap.api.security.Principals;
import co.cask.cdap.common.conf.Constants;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Exposes the system {@link ACLTable} via REST endpoints.
 */
@Path(Constants.Gateway.API_VERSION_2)
public final class ACLHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Type LIST_ACL_TYPE = new TypeToken<List<ACL>>() { }.getType();
  private static final Type LIST_PERMISSION_TYPE = new TypeToken<List<PermissionType>>() { }.getType();

  private final ACLTable aclTable;

  public ACLHandler(ACLTable aclTable) {
    this.aclTable = aclTable;
  }

  @GET
  @Path("/admin/acls/{entity-type}/{entity-id}")
  public void listByEntity(HttpRequest request, final HttpResponder responder,
                           @PathParam("entity-type") String entityTypeString,
                           @PathParam("entity-id") String entityIdString) {

    if (!entityExists(entityTypeString, entityIdString)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    EntityType entityType = EntityType.fromPluralForm(entityTypeString);
    EntityId entityId = new EntityId(entityType, entityIdString);
    responder.sendJson(HttpResponseStatus.OK, aclTable.getAcls(entityId), LIST_ACL_TYPE);
  }

  @GET
  @Path("/admin/acls/{entity-type}/{entity-id}/user/{user-id}")
  public void listByEntityAndUser(HttpRequest request, final HttpResponder responder,
                                  @PathParam("entity-type") String entityTypeString,
                                  @PathParam("entity-id") String entityIdString,
                                  @PathParam("user-id") String userId) {

    if (!entityExists(entityTypeString, entityIdString) || !userExists(userId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    EntityType entityType = EntityType.fromPluralForm(entityTypeString);
    EntityId entityId = new EntityId(entityType, entityIdString);

    List<ACL> acls = Lists.newArrayList();
    Principal user = new Principal(PrincipalType.USER, userId);
    List<ACL> userAcls = aclTable.getAcls(entityId, user);
    if (!userAcls.isEmpty()) {
      // if ACLs exist for the user, only use those ACLs
      acls.addAll(userAcls);
    } else {
      // if no ACLs exist for the user, only use the user's group ACLs
      // TODO: get groups that user is in
      List<String> groups = Lists.newArrayList();
      acls.addAll(aclTable.getAcls(entityId, Principals.fromIds(PrincipalType.GROUP, groups)));
    }

    responder.sendJson(HttpResponseStatus.OK, acls, LIST_ACL_TYPE);
  }

  @PUT
  @Path("/admin/acls/{entity-type}/{entity-id}/user/{user-id}")
  public void setAclForUser(HttpRequest request, final HttpResponder responder,
                            @PathParam("entity-type") String entityTypeString,
                            @PathParam("entity-id") String entityIdString,
                            @PathParam("user-id") String userId) {

    if (!entityExists(entityTypeString, entityIdString) || !userExists(userId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    EntityType entityType = EntityType.fromPluralForm(entityTypeString);
    EntityId entityId = new EntityId(entityType, entityIdString);

    List<PermissionType> permissions = GSON.fromJson(
      request.getContent().toString(Charsets.UTF_8), LIST_PERMISSION_TYPE);
    aclTable.setAcl(new Principal(PrincipalType.USER, userId), entityId, permissions);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @PUT
  @Path("/admin/acls/{entity-type}/{entity-id}/group/{group-id}")
  public void setAclForGroup(HttpRequest request, final HttpResponder responder,
                             @PathParam("entity-type") String entityTypeString,
                             @PathParam("entity-id") String entityIdString,
                             @PathParam("group-id") String groupId) {

    if (!entityExists(entityTypeString, entityIdString) || !groupExists(groupId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    EntityType entityType = EntityType.fromPluralForm(entityTypeString);
    EntityId entityId = new EntityId(entityType, entityIdString);

    List<PermissionType> permissions = GSON.fromJson(
      request.getContent().toString(Charsets.UTF_8), LIST_PERMISSION_TYPE);
    aclTable.setAcl(new Principal(PrincipalType.GROUP, groupId), entityId, permissions);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private boolean groupExists(String groupId) {
    return true;
  }

  private boolean userExists(String userId) {
    return true;
  }

  private boolean entityExists(String entityTypeString, String entityIdString) {
    EntityType entityType;
    try {
      entityType = EntityType.fromPluralForm(entityTypeString);
    } catch (IllegalArgumentException e) {
      return false;
    }

    EntityId entityId = new EntityId(entityType, entityIdString);
    return true;
  }
}
