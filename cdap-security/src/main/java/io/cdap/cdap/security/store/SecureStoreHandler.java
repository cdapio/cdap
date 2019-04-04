/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.security.SecureKeyCreateRequest;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Exposes REST APIs for {@link SecureStore} and
 * {@link SecureStoreManager}.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/securekeys")
public class SecureStoreHandler extends AbstractHttpHandler {
  private static final Type REQUEST_TYPE = new TypeToken<SecureKeyCreateRequest>() { }.getType();
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
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void create(FullHttpRequest httpRequest, HttpResponder httpResponder,
                     @PathParam("namespace-id") String namespace,
                     @PathParam("key-name") String name) throws Exception {
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    SecureKeyCreateRequest secureKeyCreateRequest;
    try {
      secureKeyCreateRequest = parseBody(httpRequest);
    } catch (IOException e) {
      SecureKeyCreateRequest dummy = new SecureKeyCreateRequest("<description>", "<data>",
                                                                ImmutableMap.of("key", "value"));
      throw new BadRequestException("Unable to parse the request. The request body should be of the following format." +
                                      " \n" + GSON.toJson(dummy));
    }

    if (Strings.isNullOrEmpty(secureKeyCreateRequest.getData()) || secureKeyCreateRequest.getData().trim().isEmpty()) {
      throw new BadRequestException("The data field must not be null or empty. The data will be stored securely " +
                                      "under provided key name.");
    }

    secureStoreManager.put(namespace, name, secureKeyCreateRequest.getData(),
                           secureKeyCreateRequest.getDescription(), secureKeyCreateRequest.getProperties());
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/{key-name}")
  @DELETE
  public void delete(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                     @PathParam("key-name") String name) throws Exception {
    secureStoreManager.delete(namespace, name);
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/{key-name}")
  @GET
  public void get(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                  @PathParam("key-name") String name) throws Exception {
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    httpResponder.sendByteArray(HttpResponseStatus.OK, secureStore.get(namespace, name).get(),
                                new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "text/plain;charset=utf-8"));
  }

  @Path("/{key-name}/metadata")
  @GET
  public void getMetadata(HttpRequest httpRequest, HttpResponder httpResponder,
                          @PathParam("namespace-id") String namespace,
                          @PathParam("key-name") String name) throws Exception {
    SecureStoreData secureStoreData = secureStore.get(namespace, name);
    httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(secureStoreData.getMetadata()));
  }

  @Path("/")
  @GET
  public void list(HttpRequest httpRequest, HttpResponder httpResponder,
                   @PathParam("namespace-id") String namespace) throws Exception {
    httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(secureStore.list(namespace)));
  }

  private SecureKeyCreateRequest parseBody(FullHttpRequest request) throws IOException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      throw new IOException("Unable to read contents of the request.");
    }

    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      return GSON.fromJson(reader, REQUEST_TYPE);
    }
  }
}
