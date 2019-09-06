/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.security.store.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.common.SecureKeyAlreadyExistsException;
import io.cdap.cdap.common.SecureKeyNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.security.SecureKeyCreateRequest;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The client side implementation of {@link SecureStore} and {@link SecureStoreManager} which uses rest apis to get
 * sensitive data. This class is meant to be used internally and should not be exposed to end user.
 */
public class RemoteSecureStore implements SecureStoreManager, SecureStore {
  private static final Type LIST_TYPE = new TypeToken<List<SecureStoreMetadata>>() { }.getType();
  private static final Gson GSON = new Gson();
  private final RemoteClient remoteClient;

  @VisibleForTesting
  @Inject
  RemoteSecureStore(DiscoveryServiceClient discoveryServiceClient) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.SECURE_STORE_SERVICE,
                                         new DefaultHttpRequestConfig(false), "/v3/namespaces/");
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET, createPath(namespace)).build();
    HttpResponse response = remoteClient.execute(request);
    handleResponse(response, namespace, "", "Error occurred while listing keys");
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_TYPE);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    // 1. Get metadata of the secure key
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET,
                                                      createPath(namespace, name) + "/metadata").build();
    HttpResponse response = remoteClient.execute(request);
    handleResponse(response, namespace, name, String.format("Error occurred while getting metadata for key %s:%s",
                                                            namespace, name));
    SecureStoreMetadata metadata = GSON.fromJson(response.getResponseBodyAsString(), SecureStoreMetadata.class);

    // 2. Get sensitive data for the secure key
    request = remoteClient.requestBuilder(HttpMethod.GET, createPath(namespace, name)).build();
    response = remoteClient.execute(request);
    handleResponse(response, namespace, name, String.format("Error occurred while getting key %s:%s",
                                                            namespace, name));
    // response is not a json object
    byte[] data = response.getResponseBody();
    return new SecureStoreData(metadata, data);
  }

  @Override
  public void put(String namespace, String name, String data, @Nullable String description,
                  Map<String, String> properties) throws Exception {
    SecureKeyCreateRequest createRequest = new SecureKeyCreateRequest(description, data, properties);
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, createPath(namespace, name))
      .withBody(GSON.toJson(createRequest)).build();
    HttpResponse response = remoteClient.execute(request);
    handleResponse(response, namespace, name, String.format("Error occurred while putting key %s:%s", namespace, name));
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.DELETE, createPath(namespace, name)).build();
    HttpResponse response = remoteClient.execute(request);
    handleResponse(response, namespace, name, String.format("Error occurred while deleting key %s:%s",
                                                            namespace, name));
  }

  /**
   * Handles error based on response code.
   */
  private void handleResponse(final HttpResponse response, String namespace, String name,
                              String errorPrefix) throws Exception {
    int responseCode = response.getResponseCode();
    switch (responseCode) {
      case HttpURLConnection.HTTP_OK:
        return;
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
      case HttpURLConnection.HTTP_CONFLICT:
        throw new SecureKeyAlreadyExistsException(new SecureKeyId(namespace, name));
      case HttpURLConnection.HTTP_BAD_REQUEST:
        throw new IllegalArgumentException(errorPrefix + ". Reason: " + response.getResponseBodyAsString());
      default:
        throw new IOException(errorPrefix + ". Reason: " + response.getResponseBodyAsString());
    }
  }

  /**
   * Creates the URL path for making HTTP requests for the given key name in provided namespace.
   */
  private String createPath(String namespace, String name) {
    return namespace + "/securekeys/" + name;
  }

  /**
   * Creates the URL path for making HTTP requests for the given namespace.
   */
  private String createPath(String namespace) {
    return namespace + "/securekeys";
  }
}
