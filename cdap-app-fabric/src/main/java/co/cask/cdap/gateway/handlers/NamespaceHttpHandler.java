/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
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
  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final NamespaceAdmin namespaceAdmin;

  @Inject
  public NamespaceHttpHandler(Authenticator authenticator, NamespaceAdmin namespaceAdmin) {
    super(authenticator);
    this.namespaceAdmin = namespaceAdmin;
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) {
    try {
      responder.sendJson(HttpResponseStatus.OK, namespaceAdmin.listNamespaces());
    } catch (Exception e) {
      LOG.error("Internal error while listing all namespaces", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}")
  public void getNamespace(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) {
    try {
      NamespaceMeta ns = namespaceAdmin.getNamespace(Id.Namespace.from(namespaceId));
      responder.sendJson(HttpResponseStatus.OK, ns);
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found", namespaceId));
    } catch (Exception e) {
      LOG.error("Internal error while getting namespace '{}'", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }


  @PUT
  @Path("/namespaces/{namespace-id}/properties")
  public void updateNamespaceProperties(HttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId) {
    try {
      Map<String, String> properties = GSON.fromJson(request.getContent().toString(Charsets.UTF_8), STRING_MAP_TYPE);
      namespaceAdmin.updateProperties(Id.Namespace.from(namespaceId), properties);
      responder.sendString(HttpResponseStatus.OK, "Properties updated");
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found", namespaceId));
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
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
      return;
    }

    if (!isValid(namespaceId)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Namespace id can contain only alphanumeric characters, '-' or '_'.");
      return;
    }

    if (isReserved(namespaceId)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("Cannot delete the namespace '%s'. '%s' is a reserved namespace.",
                                         namespaceId, namespaceId));
      return;
    }

    // Handle optional params
    String name = null;
    String description = null;
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
      namespaceAdmin.createNamespace(builder.build());
      responder.sendString(HttpResponseStatus.OK,
                           String.format("Namespace '%s' created successfully.", namespaceId));
    } catch (AlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.OK, String.format("Namespace '%s' already exists.", namespaceId));
    } catch (Exception e) {
      LOG.error("Internal error while creating namespace.", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @DELETE
  @Path("/namespaces/{namespace-id}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespace) {
    if (isReserved(namespace)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           String.format("Cannot delete the namespace '%s'. '%s' is a reserved namespace.",
                                         namespace, namespace));
      return;
    }
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    try {
      namespaceAdmin.deleteNamespace(namespaceId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found.", namespace));
    } catch (Exception e) {
      LOG.error("Internal error while deleting namespace.", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
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
    return Constants.DEFAULT_NAMESPACE.equals(namespaceId) || Constants.SYSTEM_NAMESPACE.equals(namespaceId) ||
      Constants.Logging.SYSTEM_NAME.equals(namespaceId);
  }
}
