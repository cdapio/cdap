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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST calls to namespace endpoints.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class NamespaceHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceHttpHandler.class);

  private final Store store;

  @Inject
  public NamespaceHttpHandler(Authenticator authenticator, StoreFactory storeFactory) {
    super(authenticator);
    this.store = storeFactory.create();
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) {
    try {
      responder.sendJson(HttpResponseStatus.OK, store.listNamespaces());
    } catch (Exception e) {
      LOG.error("Internal error while listing all namespaces", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/namespaces/{namespace}")
  public void getNamespace(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    try {
      NamespaceMeta ns = store.getNamespace(Id.Namespace.from(namespace));
      if (ns == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found", namespace));
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, ns);
    } catch (Exception e) {
      LOG.error("Internal error while getting namespace '{}'", namespace, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @PUT
  @Path("/namespaces")
  public void create(HttpRequest request, HttpResponder responder) {
    NamespaceMeta metadata;
    try {
      metadata = parseBody(request, NamespaceMeta.class);
    } catch (JsonSyntaxException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid json object provided in request body.");
      return;
    } catch (IOException e) {
      LOG.error("Failed to read namespace metadata request body.", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    String name = metadata.getName();
    // name cannot be null or empty.
    if (name == null || name.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Namespace name cannot be null or empty.");
      return;
    }
    // displayName and description could be null
    String displayName = metadata.getDisplayName();
    if (displayName == null || displayName.isEmpty()) {
      displayName = name;
    }
    String description = metadata.getDescription();
    if (description == null) {
      description = "";
    }

    try {
      NamespaceMeta existing = store.createNamespace(new NamespaceMeta.Builder().setName(name)
                                                       .setDisplayName(displayName).setDescription(description)
                                                       .build());
      if (existing == null) {
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendString(HttpResponseStatus.CONFLICT, String.format("Namespace %s already exists.", name));
      }

    } catch (Exception e) {
      LOG.error("Internal error while creating namespace.", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/namespaces/{namespace}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    try {
      // Store#deleteNamespace already checks for existence
      NamespaceMeta deletedNamespace = store.deleteNamespace(namespaceId);
      if (deletedNamespace == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found.", namespace));
      } else {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (Exception e) {
      LOG.error("Internal error while deleting namespace.", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
