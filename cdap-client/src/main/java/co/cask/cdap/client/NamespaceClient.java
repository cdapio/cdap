/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.CannotBeDeletedException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.common.exception.NamespaceAlreadyExistsException;
import co.cask.cdap.common.exception.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Client to interact with CDAP namespaces
 */
public class NamespaceClient {
  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public NamespaceClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(this.config);
  }

  /**
   * Lists all namespaces.
   *
   * @return a list of {@link NamespaceMeta} for each namespace in CDAP.
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public List<NamespaceMeta> list() throws IOException, UnAuthorizedAccessTokenException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURLV3("namespaces"),
                                               config.getAccessToken());

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<NamespaceMeta>>() {
    }).getResponseObject();
  }

  /**
   * Retrieves details about a given namespace.
   *
   * @param namespace id of the namespace for which details are requested.
   * @return
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NamespaceNotFoundException if the specified namespace is not found
   */
  public NamespaceMeta get(Id.Namespace namespace)
    throws IOException, UnAuthorizedAccessTokenException, NamespaceNotFoundException {

    HttpResponse response = restClient.execute(HttpMethod.GET,
                                               config.resolveURLV3("namespaces/%s", namespace.getId()),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NamespaceNotFoundException(namespace);
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<NamespaceMeta>() { }).getResponseObject();
  }

  /**
   * * Deletes a namespace from CDAP.
   *
   * @param namespace id of the namespace to be deleted.
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the specified namespace is not found
   * @throws CannotBeDeletedException if the specified namespace is reserved and cannot be deleted
   */
  public void delete(Id.Namespace namespace)
    throws IOException, UnAuthorizedAccessTokenException, NotFoundException, CannotBeDeletedException {

    HttpResponse response = restClient.execute(HttpMethod.DELETE,
                                               config.resolveURLV3("namespaces/%s", namespace.getId()),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_FORBIDDEN);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NamespaceNotFoundException(namespace);
    }
    if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new NamespaceCannotBeDeletedException(namespace);
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
    throws IOException, UnAuthorizedAccessTokenException, NamespaceAlreadyExistsException, BadRequestException {

    Id.Namespace namespace = Id.Namespace.from(namespaceMeta.getId());
    URL url = config.resolveURLV3("namespaces/%s", namespace.getId());
    JsonObject jsonObject = new JsonObject();
    String name = namespaceMeta.getName();
    String description = namespaceMeta.getDescription();
    if (name != null) {
      jsonObject.addProperty("name", name);
    }
    if (description != null) {
      jsonObject.addProperty("description", description);
    }
    String body = GSON.toJson(jsonObject);
    HttpRequest request = HttpRequest.put(url).withBody(body).build();
    HttpResponse response = restClient.upload(request, config.getAccessToken(), HttpURLConnection.HTTP_BAD_REQUEST);
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    // TODO: refactor this
    if (responseBody != null && responseBody.equals(String.format("Namespace '%s' already exists.",
                                                                  namespaceMeta.getId()))) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }
}
