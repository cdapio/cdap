/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.store.NamespaceStore;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST
 */
@Path(AbstractRemoteSystemOpsHandler.VERSION + "/execute")
public class RemoteNamespaceQueryHandler extends AbstractRemoteSystemOpsHandler {

  private final NamespaceStore namespaceStore;

  @Inject
  RemoteNamespaceQueryHandler(NamespaceStore namespaceStore) {
    this.namespaceStore = namespaceStore;
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,  namespaceStore.list());
  }

  @GET
  @Path("/namespaces/{namespace-id}")
  public void getNamespace(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId)
    throws Exception {
    NamespaceMeta meta = namespaceStore.get(Id.Namespace.from(namespaceId));
    if (meta == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s was not found.", namespaceId));
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, meta);
  }
}
