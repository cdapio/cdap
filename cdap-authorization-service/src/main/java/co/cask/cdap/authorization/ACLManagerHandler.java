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
package co.cask.cdap.authorization;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.authorization.ObjectIds;
import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.client.ACLStoreSupplier;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handler for getting and setting ACL entries.
 *
 * TODO: SSL and authorization for this
 */
@Path("/v1")
public class ACLManagerHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ACLManagerHandler.class);
  private static final Gson GSON = new Gson();

  private final Supplier<ACLStore> aclStoreSupplier;

  @Inject
  public ACLManagerHandler(ACLStoreSupplier aclStoreSupplier) {
    this.aclStoreSupplier = aclStoreSupplier;
  }

  public ACLManagerHandler(ACLStore aclStore) {
    this.aclStoreSupplier = Suppliers.ofInstance(aclStore);
  }

  @GET
  @Path("/acls/global")
  public void getGlobalACLs(HttpRequest request, HttpResponder responder,
                            @QueryParam("subject") String subjectString,
                            @QueryParam("permission") String permissionString) {

    // TODO: validate input
    SubjectId subject = string2SubjectId(subjectString);
    ObjectId objectId = ObjectId.GLOBAL;
    Permission permission = permissionString != null ? Permission.fromName(permissionString) : null;

    try {
      Set<ACLEntry> result = getACLStore().search(new ACLStore.Query(objectId, subject, permission));
      String response = GSON.toJson(result);
      responder.sendString(HttpResponseStatus.OK, response);
    } catch (Exception e) {
      LOG.error("Error getting ACLs matching subject={} permission={}",
                subjectString, permissionString, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/acls/namespace/{namespace-id}")
  public void getACLs(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @QueryParam("object") String objectString,
                      @QueryParam("subject") String subjectString,
                      @QueryParam("permission") String permissionString) {

    // TODO: validate input
    SubjectId subject = string2SubjectId(subjectString);
    ObjectId namespace = ObjectIds.namespace(namespaceId);
    ObjectId objectId = objectString == null ? namespace : string2ObjectId(namespace, objectString);
    Permission permission = permissionString != null ? Permission.fromName(permissionString) : null;

    try {
      Set<ACLEntry> result = getACLStore().search(new ACLStore.Query(objectId, subject, permission));
      String response = GSON.toJson(result);
      responder.sendString(HttpResponseStatus.OK, response);
    } catch (Exception e) {
      LOG.error("Error getting ACLs matching namespace={} object={} subject={} permission={}",
                namespaceId, objectString, subjectString, permissionString, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/acls")
  public void listACLs(HttpRequest request, HttpResponder responder) {
    try {
      Set<ACLEntry> result = getACLStore().search(new ACLStore.Query(null, null, null));
      String response = GSON.toJson(result);
      responder.sendString(HttpResponseStatus.OK, response);
    } catch (Exception e) {
      LOG.error("Error listing all ACLs", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/acls/global")
  public void deleteGlobalACLs(HttpRequest request, HttpResponder responder,
                               @QueryParam("subject") String subjectString,
                               @QueryParam("permission") String permissionString) {

    // TODO: validate input
    SubjectId subject = string2SubjectId(subjectString);
    ObjectId objectId = ObjectId.GLOBAL;
    Permission permission = permissionString != null ? Permission.fromName(permissionString) : null;

    try {
      getACLStore().delete(new ACLStore.Query(objectId, subject, permission));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error deleting ACLs matching subject={} permission={}", subjectString, permissionString, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/acls/namespace/{namespace-id}")
  public void deleteACLs(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @QueryParam("object") String objectString,
                         @QueryParam("subject") String subjectString,
                         @QueryParam("permission") String permissionString) {

    // TODO: validate input
    SubjectId subject = string2SubjectId(subjectString);
    ObjectId namespace = ObjectIds.namespace(namespaceId);
    ObjectId objectId = objectString == null ? namespace : string2ObjectId(namespace, objectString);
    Permission permission = permissionString != null ? Permission.fromName(permissionString) : null;

    try {
      getACLStore().delete(new ACLStore.Query(objectId, subject, permission));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error deleting ACLs matching namespace={} object={} subject={} permission={}",
                namespaceId, objectString, subjectString, permissionString, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/acls/global")
  public void createGlobalACL(HttpRequest request, HttpResponder responder) {
    String body = Bytes.toString(request.getContent().toByteBuffer());
    ACLEntry aclEntry = GSON.fromJson(body, ACLEntry.class);
    aclEntry.setObject(ObjectId.GLOBAL);

    try {
      getACLStore().write(aclEntry);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error creating global ACL: {}", aclEntry, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/acls/namespace/{namespace-id}")
  public void createACL(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
    ObjectId namespace = ObjectIds.namespace(namespaceId);

    String body = Bytes.toString(request.getContent().toByteBuffer());
    ACLEntry aclEntry = GSON.fromJson(body, ACLEntry.class);
    if (aclEntry.getObject() == null) {
      aclEntry.setObject(namespace);
    } else {
      aclEntry.getObject().setParent(namespace);
    }

    try {
      getACLStore().write(aclEntry);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error creating ACL in '{}' namespace: {}", namespaceId, aclEntry, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private SubjectId string2SubjectId(String subjectIdString) {
    if (subjectIdString == null) {
      return null;
    }

    String[] split = subjectIdString.split(":");
    if (split.length == 2) {
      return new SubjectId(split[0], split[1]);
    }

    throw new IllegalArgumentException("Invalid subjectId format: " + subjectIdString);
  }

  private ObjectId string2ObjectId(ObjectId parent, String objectIdString) {
    if (objectIdString == null) {
      return null;
    }

    if (ObjectId.GLOBAL.getType().equals(objectIdString)) {
      return ObjectId.GLOBAL;
    }

    String[] split = objectIdString.split(":");
    if (split.length == 1) {
      return new ObjectId(parent, split[0], "");
    } else if (split.length == 2) {
      return new ObjectId(parent, split[0], split[1]);
    }

    throw new IllegalArgumentException("Invalid objectId format: " + objectIdString);
  }

  private ACLStore getACLStore() {
    ACLStore aclStore = aclStoreSupplier.get();
    Preconditions.checkNotNull(aclStore);
    return aclStore;
  }
}
