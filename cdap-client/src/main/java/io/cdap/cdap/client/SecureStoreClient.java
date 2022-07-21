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

package io.cdap.cdap.client;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.SecureKeyAlreadyExistsException;
import io.cdap.cdap.common.SecureKeyNotFoundException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.security.SecureKeyCreateRequest;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Provides ways to get/set Secure keys.
 */
public class SecureStoreClient {
  private static final Gson GSON = new Gson();
  private static final String SECURE_KEYS = "securekeys";

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public SecureStoreClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public SecureStoreClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Creates a secure key
   *
   * @param secureKeyId {@link SecureKeyId} secure key name
   * @param keyCreateRequest {@link SecureKeyCreateRequest}
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws SecureKeyAlreadyExistsException if the secure key already exists
   * @throws NamespaceNotFoundException if namespace is not found
   */
  public void createKey(SecureKeyId secureKeyId, SecureKeyCreateRequest keyCreateRequest) throws IOException,
    UnauthenticatedException, AlreadyExistsException, NamespaceNotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(secureKeyId.getParent(), getSecureKeyPath(secureKeyId));
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(keyCreateRequest), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_CONFLICT);
    if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new SecureKeyAlreadyExistsException(secureKeyId);
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(secureKeyId.getParent());
    }
  }

  /**
   * Fetch the data associated with the given secure key
   *
   * @param secureKeyId {@link SecureKeyId} secure key name
   * @return the secure data in a utf-8 encoded byte array
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws SecureKeyNotFoundException if secure key or the namespace is not found
   */
  public String getData(SecureKeyId secureKeyId) throws IOException, UnauthenticatedException,
    SecureKeyNotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(secureKeyId.getParent(), getSecureKeyPath(secureKeyId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new SecureKeyNotFoundException(secureKeyId);
    }
    return response.getResponseBodyAsString();
  }

  /**
   * Get the metadata associated with the given secure key
   *
   * @param secureKeyId {@link SecureKeyId} secure key name
   * @return {@link SecureStoreMetadata} metadata associated with the key
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws SecureKeyNotFoundException if secure key or the namespace is not found
   */
  public SecureStoreMetadata getKeyMetadata(SecureKeyId secureKeyId) throws IOException, UnauthenticatedException,
    SecureKeyNotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(secureKeyId.getParent(),
                                            String.format("%s/metadata", getSecureKeyPath(secureKeyId)));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new SecureKeyNotFoundException(secureKeyId);
    }
    return GSON.fromJson(response.getResponseBodyAsString(), SecureStoreMetadata.class);
  }

  /**
   * Delete the secure key
   * @param secureKeyId {@link SecureKeyId} secure key name
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws SecureKeyNotFoundException if secure key or the namespace is not found
   */
  public void deleteKey(SecureKeyId secureKeyId) throws IOException, UnauthenticatedException,
    SecureKeyNotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(secureKeyId.getParent(), getSecureKeyPath(secureKeyId));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new SecureKeyNotFoundException(secureKeyId);
    }
  }

  /**
   * List all the secure keys in the namespace
   * @param namespaceId {@link NamespaceId} namespace id
   * @return list of key names and descriptions
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NamespaceNotFoundException if the given namespace is not found
   */
  public List<SecureStoreMetadata> listKeys(NamespaceId namespaceId) throws IOException, UnauthenticatedException,
    NamespaceNotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(namespaceId, SECURE_KEYS);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<SecureStoreMetadata>>() { }).getResponseObject();
  }

  private static String getSecureKeyPath(SecureKeyId secureKeyId) {
    return String.format("%s/%s", SECURE_KEYS, secureKeyId.getName());
  }
}
