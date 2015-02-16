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
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.CharMatcher;
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
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;

  @Inject
  public NamespaceHttpHandler(Authenticator authenticator, StoreFactory storeFactory,
                              PreferencesStore preferencesStore, DashboardStore dashboardStore,
                              SecureHandler secureHandler) {
    super(authenticator, secureHandler);
    this.store = storeFactory.create();
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
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
  @Path("/namespaces/{namespace-id}")
  public void getNamespace(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) {
    try {
      NamespaceMeta ns = store.getNamespace(Id.Namespace.from(namespaceId));
      if (ns == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found", namespaceId));
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, ns);
    } catch (Exception e) {
      LOG.error("Internal error while getting namespace '{}'", namespaceId, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @PUT
  @Path("/namespaces/{namespace-id}")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
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

    if (!isValid(namespaceId)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Namespace id can contain only alphanumeric characters, '-' or '_'.");
      return;
    }

    if (isReserved(namespaceId)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("Cannot create namespace %s. '%s' and '%s' are reserved namespaces.",
                                         namespaceId,
                                         Constants.DEFAULT_NAMESPACE,
                                         Constants.SYSTEM_NAMESPACE));
      return;
    }

    // Handle optional params
    // name defaults to id
    String name = namespaceId;
    // description defaults to empty
    String description = "";
    // override optional params if they are provided in the request
    if (metadata != null) {
      if (metadata.getName() != null) {
        name = metadata.getName();
      }
      if (metadata.getDescription() != null) {
        description = metadata.getDescription();
      }
    }

    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    builder.setId(namespaceId)
      .setName(name)
      .setDescription(description)
      .build();

    try {
      NamespaceMeta existing = store.createNamespace(builder.build());
      // make the API idempotent, but send appropriate response
      String response;
      if (existing == null) {
        response = String.format("Namespace '%s' created successfully.", namespaceId);
      } else {
        response = String.format("Namespace '%s' already exists.", namespaceId);
      }
      responder.sendString(HttpResponseStatus.OK, response);
    } catch (Exception e) {
      LOG.error("Internal error while creating namespace.", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/namespaces/{namespace-id}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespace) {
    if (isReserved(namespace)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           String.format("Cannot delete namespace '%s'. '%s' and '%s' are reserved namespaces.",
                                         namespace,
                                         Constants.DEFAULT_NAMESPACE,
                                         Constants.SYSTEM_NAMESPACE));
      return;
    }
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    try {
      // Delete Preferences associated with this namespace
      preferencesStore.deleteProperties(namespace);

      // Delete all dashboards associated with this namespace
      dashboardStore.delete(namespace);

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

  private boolean isValid(String namespaceId) {
    // TODO: This is copied from StreamVerification in app-fabric as this handler is in data-fabric module.
    return CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9')).matchesAllOf(namespaceId);
  }

  private boolean isReserved(String namespaceId) {
    return Constants.DEFAULT_NAMESPACE.equals(namespaceId) || Constants.SYSTEM_NAMESPACE.equals(namespaceId);
  }
}
