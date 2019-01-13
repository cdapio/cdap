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

package co.cask.cdap.security.store.client;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.SecureKeyAlreadyExistsException;
import co.cask.cdap.common.SecureKeyNotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.Map;

/**
 * Remote secure store bindings.
 */
public class RemoteSecureStore implements SecureStoreManager, SecureStore {
  private static final HttpRequestConfig HTTP_REQUEST_CONFIG = new DefaultHttpRequestConfig();
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final RemoteClient remoteClient;

  @Inject
  public RemoteSecureStore(final DiscoveryServiceClient discoveryServiceClient) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Security.Store.SECURE_STORE_SERVICE,
                                         HTTP_REQUEST_CONFIG, "/v3/namespaces/");
  }

  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, namespace + "/" + "securekeys/").build();
    HttpResponse response = remoteClient.execute(request);
    handleError(response, namespace, namespace, "Error in listing keys ");
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, namespace + "/" + "securekeys/" + name).build();
    HttpResponse response = remoteClient.execute(request);
    handleError(response, namespace, namespace, "Error in getting key ");
    // response is not json but just plain text data
    byte[] data = response.getResponseBody();
    request = remoteClient.requestBuilder(HttpMethod.PUT, namespace + "/" + "securekeys/" + name + "/metadata").build();
    response = remoteClient.execute(request);
    handleError(response, namespace, namespace, "Error in getting key ");
    SecureStoreMetadata metadata = GSON.fromJson(response.getResponseBodyAsString(), SecureStoreMetadata.class);
    return new SecureStoreData(metadata, data);
  }

  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    SecureKeyCreateRequest createRequest = new SecureKeyCreateRequest(description, data, properties);
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, namespace + "/" + "securekeys/" + name)
      .withBody(GSON.toJson(createRequest))
      .build();
    HttpResponse response = remoteClient.execute(request);
    handleError(response, namespace, name, "Failed to write secure key.");
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.DELETE, namespace + "/" + "securekeys/" + name)
      .build();
    HttpResponse response = remoteClient.execute(request);
    handleError(response, namespace, name, "Failed to delete key");
  }

  /**
   * Handles error response based on the given response code.
   */
  private void handleError(final HttpResponse response, String namespace, String name,
                           String errorPrefix) throws Exception {
    int responseCode = response.getResponseCode();
    switch (responseCode) {
      case HttpURLConnection.HTTP_OK:
        return;
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
      case HttpURLConnection.HTTP_CONFLICT:
        throw new SecureKeyAlreadyExistsException(new SecureKeyId(namespace, name));
      case HttpURLConnection.HTTP_UNAVAILABLE:
        throw new ServiceUnavailableException(Constants.Security.Store.SECURE_STORE_SERVICE);
      default:
        throw new IOException(errorPrefix + ". Reason: " + response.getResponseBodyAsString());
    }
  }
}
