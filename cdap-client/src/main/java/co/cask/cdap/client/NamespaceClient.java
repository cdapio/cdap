/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.AlreadyExistsException;
import co.cask.cdap.client.exception.BadRequestException;
import co.cask.cdap.client.exception.CannotBeDeletedException;
import co.cask.cdap.client.exception.NotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Client to interact with CDAP namespaces
 */
public class NamespaceClient {
  private final RESTClient restClient;
  private final ClientConfig config;

  private static final String NAMESPACE_ENTITY_TYPE = "namespace";

  @Inject
  public NamespaceClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Lists all namespaces in CDAP.
   *
   * @return a list of {@link NamespaceMeta} for each namespace in CDAP.
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public List<NamespaceMeta> list() throws IOException, UnAuthorizedAccessTokenException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURL("namespaces"),
                                               config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<NamespaceMeta>>() { }).getResponseObject();
  }

  /**
   * Retrieves details about a given namespace.
   *
   * @param namespaceId id of the namespace for which details are requested.
   * @return
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the specified namespace is not found
   */
  public NamespaceMeta get(String namespaceId) throws IOException, UnAuthorizedAccessTokenException, NotFoundException {
    HttpResponse response = restClient.execute(HttpMethod.GET,
                                               config.resolveURL(String.format("namespaces/%s", namespaceId)),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(NAMESPACE_ENTITY_TYPE, namespaceId);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<NamespaceMeta>() { }).getResponseObject();
  }

  /**
   * * Deletes a namespace from CDAP.
   *
   * @param namespaceId id of the namespace to be deleted.
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the specified namespace is not found
   * @throws CannotBeDeletedException if the specified namespace is reserved and cannot be deleted
   */
  public void delete(String namespaceId) throws IOException, UnAuthorizedAccessTokenException, NotFoundException,
    CannotBeDeletedException {
    HttpResponse response = restClient.execute(HttpMethod.DELETE,
                                               config.resolveURL(String.format("namespaces/%s", namespaceId)),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_FORBIDDEN);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(NAMESPACE_ENTITY_TYPE, namespaceId);
    }
    if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new CannotBeDeletedException(NAMESPACE_ENTITY_TYPE, namespaceId);
    }
  }

  /**
   * Creates a new namespace in CDAP
   *
   * @param namespaceMeta the {@link NamespaceMeta} for the namespace to be created
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws AlreadyExistsException if the specified namespace already exists
   * @throws BadRequestException if the specified namespace contains an invalid or reserved namespace id
   */
  public void create(NamespaceMeta namespaceMeta)
    throws IOException, UnAuthorizedAccessTokenException, AlreadyExistsException, BadRequestException {
    URL url = config.resolveURL(String.format("namespaces/%s", namespaceMeta.getId()));
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    String displayName = namespaceMeta.getDisplayName();
    String description = namespaceMeta.getDescription();
    if (displayName != null) {
      builder.setDisplayName(displayName);
    }
    if (description != null) {
      builder.setDescription(description);
    }
    String body = new Gson().toJson(builder.build());
    HttpRequest request = HttpRequest.put(url).withBody(body).build();
    HttpResponse response = restClient.upload(request, config.getAccessToken(), HttpURLConnection.HTTP_BAD_REQUEST);
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    if (responseBody != null && responseBody.contains("already exists")) {
      throw new AlreadyExistsException(NAMESPACE_ENTITY_TYPE, namespaceMeta.getId());
    }
  }
}
