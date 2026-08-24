/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.security.store;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Exposes Internal REST APIs for {@link SecureStore} and {@link SecureStoreManager}.
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/namespaces/{namespace-id}/securekeys")
public class SecureStoreInternalHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();

  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  public SecureStoreInternalHandler(SecureStore secureStore, SecureStoreManager secureStoreManager) {
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  @Path("/storeInfo")
  @GET
  public void getStoreInfo(HttpRequest httpRequest, HttpResponder httpResponder,
      @PathParam("namespace-id") String namespace) throws Exception {
    httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(secureStore.getStoreInfo()));
  }

  @Path("/{key-name}/acquireLease")
  @POST
  public void acquireLease(HttpRequest httpRequest, HttpResponder httpResponder,
      @PathParam("namespace-id") String namespace,
      @PathParam("key-name") String name,
      @QueryParam("timeoutMs") long timeoutMs,
      @QueryParam("leaseHolder") String leaseHolder) throws Exception {
    boolean acquired = secureStoreManager.acquireLease(namespace, name, timeoutMs, leaseHolder);
    httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(acquired));
  }

  @Path("/{key-name}/releaseLease")
  @POST
  public void releaseLease(HttpRequest httpRequest, HttpResponder httpResponder,
      @PathParam("namespace-id") String namespace,
      @PathParam("key-name") String name,
      @QueryParam("leaseHolder") String leaseHolder) throws Exception {
    boolean released = secureStoreManager.releaseLease(namespace, name, leaseHolder);
    httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(released));
  }
}
