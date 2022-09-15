/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.store.state.AppState;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Optional;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Internal {@link HttpHandler} for Application State Management
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class AppStateHandler extends AbstractHttpHandler {
  private final ApplicationLifecycleService applicationLifecycleService;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public AppStateHandler(ApplicationLifecycleService applicationLifecycleService,
                         NamespaceQueryAdmin namespaceQueryAdmin) {
    this.applicationLifecycleService = applicationLifecycleService;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * Get {@link AppState} for a given app-name.
   */
  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-name}/states/{state-key}")
  public void getState(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-name") String appName,
                       @PathParam("state-key") String stateKey) throws Exception {
    validateInput(namespaceId, appName, stateKey);
    AppState appStateRequest = new AppState(namespaceId, appName, stateKey);
    Optional<byte[]> appStateResponse = applicationLifecycleService.getState(appStateRequest);
    if (appStateResponse.isPresent()) {
      responder.sendByteArray(HttpResponseStatus.OK, appStateResponse.get(),
                              EmptyHttpHeaders.INSTANCE);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  /**
   * Save {@link AppState} for a given app-name.
   */
  @PUT
  @Path("/namespaces/{namespace-id}/apps/{app-name}/states/{state-key}")
  public void saveState(FullHttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-name") String appName,
                        @PathParam("state-key") String stateKey) throws Exception {
    validateInput(namespaceId, appName, stateKey);
    byte[] stateValue = new byte[request.content().readableBytes()];
    request.content().readBytes(stateValue);
    AppState appStateRequest = new AppState(namespaceId, appName, stateKey, stateValue);
    applicationLifecycleService.saveState(appStateRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete a {@link AppState} for a given app-name.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-name}/states/{state-key}")
  public void deleteState(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("app-name") String appName,
                          @PathParam("state-key") String stateKey) throws Exception {
    validateInput(namespaceId, appName, stateKey);
    AppState appStateRequest = new AppState(namespaceId, appName, stateKey);
    applicationLifecycleService.deleteState(appStateRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete all {@link AppState} for a given app-name.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-name}/states")
  public void deleteAllStates(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("app-name") String appName) throws Exception {
    validateInput(namespaceId, appName);
    AppState appStateRequest = new AppState(namespaceId, appName);
    applicationLifecycleService.deleteAllStates(appStateRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void validateInput(String namespace, String appName) throws Exception {
    NamespaceId namespaceId = validateNamespace(namespace);

    validateApplication(appName, namespaceId);
  }

  private void validateInput(String namespace, String appName, String stateKey) throws Exception {
    NamespaceId namespaceId = validateNamespace(namespace);

    validateApplication(appName, namespaceId);

    validateStateKey(stateKey);
  }

  private void validateApplication(String appName, NamespaceId namespaceId)
    throws BadRequestException, ApplicationNotFoundException {
    if (appName == null) {
      throw new BadRequestException("Path parameter app-id cannot be empty");
    }

    ApplicationId appId = namespaceId.app(appName);
    applicationLifecycleService.validateApplication(appId);
  }

  private void validateStateKey(String stateKey) throws BadRequestException {
    if (stateKey == null) {
      throw new BadRequestException("Path parameter state-key cannot be empty");
    }
  }

  private NamespaceId validateNamespace(String namespace) throws Exception {
    if (namespace == null) {
      throw new BadRequestException("Path parameter namespace-id cannot be empty");
    }

    NamespaceId namespaceId = new NamespaceId(namespace);
    try {
      if (!namespaceId.equals(NamespaceId.SYSTEM)) {
        namespaceQueryAdmin.get(namespaceId);
      }
    } catch (NamespaceNotFoundException | AccessException e) {
      throw e;
    }
    return namespaceId;
  }
}
