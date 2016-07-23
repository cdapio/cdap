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
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.http.HttpResponder;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
  public SecureStoreHandler(SecureStore secureStore, SecureStoreManager secureStoreManager) {
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  @Path("/{key-name}")
  @PUT
  public void create(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                     @PathParam("key-name") String name) throws BadRequestException, IOException {
    if (!EntityId.isValidStoreKey(name)) {
      throw new BadRequestException("Improperly formatted secure key name. The name can contain lower case " +
                                      "alphabets, " + "numbers, _, and -");
    }
    SecureKeyCreateRequest secureKeyCreateRequest = parseBody(httpRequest, SecureKeyCreateRequest.class);

    if (secureKeyCreateRequest == null) {
      SecureKeyCreateRequest dummy = new SecureKeyCreateRequest("<description>", "<data>",
                                                                ImmutableMap.of("key", "value"));
      throw new BadRequestException("Unable to parse the request. The request body should be of the following format." +
                                      " \n" + GSON.toJson(dummy));
    }

    String description = secureKeyCreateRequest.getDescription();
    String value = secureKeyCreateRequest.getData();
    if (Strings.isNullOrEmpty(value)) {
      throw new BadRequestException("The data field should not be empty. This is the data that will be stored " +
                                      "securely.");
    }

    byte[] data = value.getBytes(StandardCharsets.UTF_8);
    secureStoreManager.putSecureData(namespace, name, data, description, secureKeyCreateRequest.getProperties());
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/{key-name}")
  @DELETE
  public void delete(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                     @PathParam("key-name") String name) throws IOException {
    secureStoreManager.deleteSecureData(namespace, name);
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/{key-name}")
  @GET
  public void get(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                  @PathParam("key-name") String name) throws IOException {
    SecureStoreData secureStoreData = secureStore.getSecureData(namespace, name);
    String data = new String(secureStoreData.get(), StandardCharsets.UTF_8);
    httpResponder.sendJson(HttpResponseStatus.OK, data);
  }

  @Path("/{key-name}/metadata")
  @GET
  public void getMetadata(HttpRequest httpRequest, HttpResponder httpResponder,
                          @PathParam("namespace-id") String namespace, @PathParam("key-name") String name)
    throws IOException {
    SecureStoreData secureStoreData = secureStore.getSecureData(namespace, name);
    httpResponder.sendJson(HttpResponseStatus.OK, secureStoreData.getMetadata());
  }

  @Path("/")
  @GET
  public void list(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace)
    throws IOException {
    List<SecureStoreMetadata> metadatas = secureStore.listSecureData(namespace);
    List<SecureKeyListEntry> returnList = new ArrayList<>(metadatas.size());
    for (SecureStoreMetadata metadata : metadatas) {
      returnList.add(new SecureKeyListEntry(metadata.getName(), metadata.getDescription()));
    }

    httpResponder.sendJson(HttpResponseStatus.OK, returnList);
  }
}
