/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.state;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * Internal {@link HttpHandler} for Application State Management
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class AppStateHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final AppStateStore appStateStore;

  @Inject
  AppStateHandler(NamespaceQueryAdmin namespaceQueryAdmin,
                  AppStateStore appStateStore) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.appStateStore = appStateStore;
  }

  /**
   * Get {@link AppState} for a given app-name.
   */
  @GET
  @Path("/namespaces/{namespace-id}/app/{app-name}")
  public void getState(FullHttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-name") String appName) throws Exception {
    validateNamespace(namespaceId);
    AppState appStateRequest = getAppStateRequest(request, namespaceId, appName);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(appStateStore.getState(appStateRequest)));
  }

  /**
   * Save {@link AppState} for a given app-name.
   */
  @POST
  @Path("/namespaces/{namespace-id}/app/{app-name}")
  public void saveState(FullHttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-name") String appName) throws Exception {
    validateNamespace(namespaceId);
    AppState appStateRequest = getAppStateRequest(request, namespaceId, appName);
    appStateStore.saveState(appStateRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete {@link AppState} for a given app-name.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/app/{app-name}")
  public void deleteState(FullHttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-name") String appName) throws Exception {
    validateNamespace(namespaceId);
    AppState appStateRequest = getAppStateRequest(request, namespaceId, appName);
    appStateStore.deleteState(appStateRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void validateNamespace(String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }

  private AppState getAppStateRequest(FullHttpRequest request, String namespace, String appName) {
    AppState appStateRequest = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
            AppState.class);
    appStateRequest.setNamespace(namespace);
    appStateRequest.setAppName(appName);
    return appStateRequest;
  }
}
