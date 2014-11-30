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
import co.cask.cdap.namespace.NamespaceMetadata;
import co.cask.cdap.namespace.NamespaceMetadataStore;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link HttpHandler} for handling REST call to namespace endpoints.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class NamespaceHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceHttpHandler.class);

  private static final Gson GSON = new Gson();

  private Store store;
  private NamespaceMetadataStore.InMemoryNamespaceMetadataStore tempStore;

  @Inject
  public NamespaceHttpHandler(Authenticator authenticator, StoreFactory storeFactory) {
    super(authenticator);
    // store = storeFactory.create();
    tempStore = NamespaceMetadataStore.InMemoryNamespaceMetadataStore.getInstance();
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) {
    LOG.debug("Lising all namespaces");
    try {
      List<NamespaceMetadata> namespaces = tempStore.getAllNamespaces();
      if (null == namespaces) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        String result = GSON.toJson(namespaces);
        responder.sendByteArray(HttpResponseStatus.OK, result.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/namespaces/{namespace}")
  public void getNamespace(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    LOG.debug("Lising namespace {}", namespace);
    NamespaceMetadata ns = tempStore.get(namespace);
    if (null == ns) {
      LOG.error("Namespace {} not found", namespace);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
    String result = GSON.toJson(tempStore.get(namespace));
    responder.sendByteArray(HttpResponseStatus.OK, result.getBytes(Charsets.UTF_8),
                            ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
  }

  @PUT
  @Path("/namespaces/{namespace}")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    LOG.debug("Creating namespace {}", namespace);
    try {
      NamespaceMetadata metadata = parseBody(request, NamespaceMetadata.class);
      tempStore.writeNamespace(namespace, metadata.getDisplayName(), metadata.getDescription());
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      LOG.error("Invalid namespace input: {}", request.getContent().toString(Charsets.UTF_8), e);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    } catch (NamespaceMetadataStore.InMemoryNamespaceMetadataStore.NamespaceAlreadyExistsException e) {
      LOG.error(e.getLocalizedMessage(), e);
      responder.sendStatus(HttpResponseStatus.CONFLICT);
    }
  }

  @DELETE
  @Path("/namespaces/{namespace}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    LOG.debug("Deleting namespace {}", namespace);
    try {
      tempStore.deleteNamespace(namespace);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NamespaceMetadataStore.InMemoryNamespaceMetadataStore.NamespaceNotFoundException e) {
      LOG.error("Namespace {} not found", namespace);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }
  }
}
