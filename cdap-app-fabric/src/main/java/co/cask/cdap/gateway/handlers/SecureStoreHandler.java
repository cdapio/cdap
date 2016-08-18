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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Exposes REST APIs for {@link co.cask.cdap.api.security.store.SecureStore} and
 * {@link co.cask.cdap.api.security.store.SecureStoreManager}.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/securekeys")
public class SecureStoreHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();

  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  SecureStoreHandler(SecureStore secureStore, SecureStoreManager secureStoreManager) {
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  @Path("/{key-name}")
  @PUT
  public void create(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                     @PathParam("key-name") String name) throws Exception {

    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    SecureKeyCreateRequest secureKeyCreateRequest = parseBody(httpRequest, SecureKeyCreateRequest.class);

    if (secureKeyCreateRequest == null) {
      SecureKeyCreateRequest dummy = new SecureKeyCreateRequest("<description>", "<data>",
                                                                ImmutableMap.of("key", "value"));
      throw new BadRequestException("Unable to parse the request. The request body should be of the following format." +
                                      " \n" + GSON.toJson(dummy));
    }

    secureStoreManager.putSecureData(namespace, name, secureKeyCreateRequest.getData(),
                                     secureKeyCreateRequest.getDescription(), secureKeyCreateRequest.getProperties());
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/{key-name}")
  @DELETE
  public void delete(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                     @PathParam("key-name") String name) throws Exception {
    secureStoreManager.deleteSecureData(namespace, name);
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/{key-name}")
  @GET
  public void get(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                  @PathParam("key-name") String name) throws Exception {
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    httpResponder.sendContent(HttpResponseStatus.OK,
                              ChannelBuffers.wrappedBuffer(secureStore.getSecureData(namespace, name).get()),
                              "text/plain; charset=utf-8", null);
  }

  @Path("/{key-name}/metadata")
  @GET
  public void getMetadata(HttpRequest httpRequest, HttpResponder httpResponder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("key-name") String name) throws Exception {
    SecureStoreData secureStoreData = secureStore.getSecureData(namespace, name);
    httpResponder.sendJson(HttpResponseStatus.OK, secureStoreData.getMetadata());
  }

  @Path("/")
  @GET
  public void list(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace)
    throws Exception {
    httpResponder.sendJson(HttpResponseStatus.OK, secureStore.listSecureData(namespace));
  }
}
