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
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.store.state.AppStateKey;
import io.cdap.cdap.internal.app.store.state.AppStateKeyValue;
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
   * Get state for a given app-name and state-key. Returns null as response if the app is valid, but
   * there is no entry for the key Returns {@link HttpResponseStatus.NOT_FOUND} if namespace or app
   * is not valid
   */
  @GET
  @Path("/namespaces/{namespace}/apps/{app-name}/states/{state-key}")
  public void getState(HttpRequest request, HttpResponder responder,
      @PathParam("namespace") String namespace,
      @PathParam("app-name") String appName,
      @PathParam("state-key") String stateKey) throws Exception {
    NamespaceId namespaceId = validateInput(namespace, appName, stateKey);
    AppStateKey appStateKeyRequest = new AppStateKey(namespaceId, appName, stateKey);
    Optional<byte[]> appStateResponse = applicationLifecycleService.getState(appStateKeyRequest);
    if (appStateResponse.isPresent()) {
      responder.sendByteArray(HttpResponseStatus.OK, appStateResponse.get(),
          EmptyHttpHeaders.INSTANCE);
    } else {
      responder.sendStatus(HttpResponseStatus.OK, EmptyHttpHeaders.INSTANCE);
    }
  }

  /**
   * Save state for a given app-name and state-key. All params and state value should be present
   */
  @PUT
  @Path("/namespaces/{namespace}/apps/{app-name}/states/{state-key}")
  public void saveState(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace") String namespace,
      @PathParam("app-name") String appName,
      @PathParam("state-key") String stateKey) throws Exception {
    byte[] stateValue = new byte[request.content().readableBytes()];
    request.content().readBytes(stateValue);

    NamespaceId namespaceId = validateInput(namespace, appName, stateKey, stateValue);
    AppStateKeyValue appStateKeyRequest = new AppStateKeyValue(namespaceId, appName, stateKey,
        stateValue);
    applicationLifecycleService.saveState(appStateKeyRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete a state for a given app-name and state-key.
   */
  @DELETE
  @Path("/namespaces/{namespace}/apps/{app-name}/states/{state-key}")
  public void deleteState(HttpRequest request, HttpResponder responder,
      @PathParam("namespace") String namespace,
      @PathParam("app-name") String appName,
      @PathParam("state-key") String stateKey)
      throws Exception {
    NamespaceId namespaceId = validateInput(namespace, appName, stateKey);
    AppStateKey appStateKeyRequest = new AppStateKey(namespaceId, appName, stateKey);
    applicationLifecycleService.deleteState(appStateKeyRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete all states for a given app-name.
   */
  @DELETE
  @Path("/namespaces/{namespace}/apps/{app-name}/states")
  public void deleteAllStates(HttpRequest request, HttpResponder responder,
      @PathParam("namespace") String namespace,
      @PathParam("app-name") String appName)
      throws Exception {
    NamespaceId namespaceId = validateInput(namespace, appName);
    applicationLifecycleService.deleteAllStates(namespaceId, appName);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private NamespaceId validateInput(String namespace, String appName)
      throws Exception {
    NamespaceId namespaceId = validateNamespace(namespace);

    validateApplication(namespaceId, appName);

    return namespaceId;
  }

  private NamespaceId validateInput(String namespace, String appName, String stateKey)
      throws Exception {
    NamespaceId namespaceId = validateInput(namespace, appName);

    validateStateKey(stateKey);

    return namespaceId;
  }

  private NamespaceId validateInput(String namespace, String appName, String stateKey,
      byte[] stateValue)
      throws Exception {
    NamespaceId namespaceId = validateInput(namespace, appName, stateKey);

    validateStateValue(stateValue);

    return namespaceId;
  }

  private void validateApplication(NamespaceId namespaceId, String appName)
      throws NullPointerException, IllegalArgumentException {
    // Verify app name is not null and has a valid app name
    namespaceId.app(appName);
  }

  private void validateStateKey(String stateKey) throws NullPointerException {
    if (stateKey == null) {
      throw new NullPointerException("Path parameter state-key cannot be empty");
    }
  }

  private void validateStateValue(byte[] stateValue) throws NullPointerException {
    if (stateValue == null) {
      throw new NullPointerException("Parameter state-value cannot be empty");
    }
  }

  private NamespaceId validateNamespace(String namespace) throws Exception {
    // Verify namespace is not null and has a valid namespace name
    NamespaceId namespaceId = new NamespaceId(namespace);

    // Verify namespace exists
    if (!namespaceId.equals(NamespaceId.SYSTEM)) {
      namespaceQueryAdmin.get(namespaceId);
    }

    return namespaceId;
  }
}
