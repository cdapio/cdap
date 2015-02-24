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
import co.cask.cdap.common.http.SecurityRequestContext;
import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.client.ACLStoreSupplier;
import co.cask.common.authorization.client.AuthorizationClient;
import co.cask.common.authorization.client.DefaultAuthorizationClient;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
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

/**
 * Handler for getting and setting ACL entries.
 *
 * TODO: SSL and authorization for this
 */
@Path("/v3")
public class ACLManagerHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ACLManagerHandler.class);
  private static final Gson GSON = new Gson();

  private final Supplier<ACLStore> aclStoreSupplier;
  private final AuthorizationClient authorizationClient;

  @Inject
  public ACLManagerHandler(ACLStoreSupplier aclStoreSupplier, AuthorizationClient authorizationClient) {
    this.aclStoreSupplier = aclStoreSupplier;
    this.authorizationClient = authorizationClient;
  }

  public ACLManagerHandler(ACLStore aclStore, AuthorizationClient authorizationClient) {
    this.aclStoreSupplier = Suppliers.ofInstance(aclStore);
    this.authorizationClient = authorizationClient;
  }

  @POST
  @Path("/acls/search")
  public void searchACLs(HttpRequest request, HttpResponder responder) {
    ACLStore.Query query;
    try {
      query = parseQuery(request);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    if (!authorizationClient.isAuthorized(query.getObjectId(), SecurityRequestContext.getSubjects(),
                                     ImmutableList.of(Permission.ADMIN))) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
      return;
    }

    try {
      Set<ACLEntry> result = getACLStore().search(query);
      String response = GSON.toJson(result);
      responder.sendString(HttpResponseStatus.OK, response);
    } catch (Exception e) {
      LOG.error("Error getting app ACLs matching query={}", query, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/acls")
  public void getACLs(HttpRequest request, HttpResponder responder) {
    if (!authorizationClient.isAuthorized(ObjectId.GLOBAL, SecurityRequestContext.getSubjects(),
                                          ImmutableList.of(Permission.ADMIN))) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
      return;
    }

    try {
      Set<ACLEntry> result = getACLStore().search(new ACLStore.Query(null, null, null));
      String response = GSON.toJson(result);
      responder.sendString(HttpResponseStatus.OK, response);
    } catch (Exception e) {
      LOG.error("Error listing all ACLs", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/acls/delete")
  public void deleteACLs(HttpRequest request, HttpResponder responder) {
    ACLStore.Query query;
    try {
      query = parseQuery(request);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    if (!authorizationClient.isAuthorized(query.getObjectId(), SecurityRequestContext.getSubjects(),
                                          ImmutableList.of(Permission.ADMIN))) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
      return;
    }

    try {
      getACLStore().delete(query);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error deleting ACLs matching {}", query.toString(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/acls")
  public void createACL(HttpRequest request, HttpResponder responder) {
    ACLEntry entry;
    try {
      entry = parseACLEntry(request);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    if (!authorizationClient.isAuthorized(entry.getObject(), SecurityRequestContext.getSubjects(),
                                          ImmutableList.of(Permission.ADMIN))) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
      return;
    }

    try {
      getACLStore().write(entry);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error creating ACL: {}", entry, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private ACLEntry parseACLEntry(HttpRequest request) throws IllegalArgumentException {
    try {
      String body = Bytes.toString(request.getContent().toByteBuffer());
      ACLEntry entry = GSON.fromJson(body, ACLEntry.class);
      if (entry.getObject() == null || entry.getSubject() == null || entry.getPermission() == null) {
        throw new IllegalArgumentException();
      }
      return entry;
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException();
    }
  }

  private ACLStore.Query parseQuery(HttpRequest request) throws IllegalArgumentException {
    try {
      String body = Bytes.toString(request.getContent().toByteBuffer());
      ACLStore.Query query = GSON.fromJson(body, ACLStore.Query.class);
      if (query.getObjectId() == null && query.getSubjectId() != null && query.getPermission() != null) {
        throw new IllegalArgumentException();
      }
      return query;
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException();
    }
  }

  private ACLStore getACLStore() {
    ACLStore aclStore = aclStoreSupplier.get();
    Preconditions.checkNotNull(aclStore);
    return aclStore;
  }
}
