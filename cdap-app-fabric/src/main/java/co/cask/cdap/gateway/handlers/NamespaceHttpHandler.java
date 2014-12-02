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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.namespace.NamespaceMetaStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link HttpHandler} for handling REST call to namespace endpoints.
 */
@Path(Constants.Gateway.API_VERSION)
public class NamespaceHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceHttpHandler.class);

  private final NamespaceMetaStore namespaceMetaStore;

  @Inject
  public NamespaceHttpHandler(Authenticator authenticator, NamespaceMetaStore namespaceMetaStore) {
    super(authenticator);
    this.namespaceMetaStore = namespaceMetaStore;
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) {
    LOG.debug("Listing all namespaces");
    try {
      List<NamespaceMeta> namespaces = namespaceMetaStore.list();
      if (null == namespaces) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendJson(HttpResponseStatus.OK, namespaces);
      }
    } catch (Exception e) {
      LOG.error("Internal error while listing all namespaces", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/namespaces/{namespace}")
  public void getNamespace(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    LOG.debug("Listing namespace {}", namespace);
    try {
      NamespaceMeta ns = namespaceMetaStore.get(Id.Namespace.from(namespace));
      if (null == ns) {
        LOG.error("Namespace {} not found", namespace);
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found", namespace));
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, ns);
    } catch (Exception e) {
      LOG.error("Internal error while listing namespace", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/namespaces")
  public void create(HttpRequest request, HttpResponder responder) {
    try {
      NamespaceMeta metadata = parseBody(request, NamespaceMeta.class);
      String name = metadata.getName();
      // name cannot be null or empty.
      if (null == name || name.isEmpty()) {
        LOG.error("Namespace name cannot be null or empty.");
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Namespace name cannot be null or empty.");
        return;
      }
      if (namespaceMetaStore.exists(Id.Namespace.from(name))) {
        LOG.error("Namespace {} already exists", name);
        responder.sendString(HttpResponseStatus.CONFLICT, String.format("Namespace %s already exists", name));
        return;
      }
      LOG.debug("Creating namespace {}", name);
      // displayName and description could be null
      String displayName = metadata.getDisplayName();
      if (null == displayName || displayName.isEmpty()) {
        displayName = name;
      }
      String description = metadata.getDescription();
      if (null == description) {
        description = "";
      }
      namespaceMetaStore.create(new NamespaceMeta.Builder().setName(name).setDisplayName(displayName)
                                  .setDescription(description).build());
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException e) {
      LOG.error("Invalid namespace metadata. Must be a valid json.", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("Invalid namespace metadata. Must be a valid" +
                                                                           " json."));
    } catch (IOException e) {
      LOG.error("Error reading namespace metadata: {}", request.getContent().toString(Charsets.UTF_8), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOG.error("Internal error while creating namespace", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/namespaces/{namespace}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    LOG.debug("Deleting namespace {}", namespace);
    try {
      Id.Namespace namespaceId = Id.Namespace.from(namespace);
      if (!namespaceMetaStore.exists(namespaceId)) {
        LOG.error("Namespace {} not found", namespace);
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not found", namespace));
        return;
      }
      namespaceMetaStore.delete(namespaceId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Internal error while deleting namespace ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
