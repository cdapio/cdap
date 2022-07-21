/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.DatasetModuleAlreadyExistsException;
import io.cdap.cdap.common.DatasetModuleCannotBeDeletedException;
import io.cdap.cdap.common.DatasetModuleNotFoundException;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Dataset modules.
 */
@Beta
public class DatasetModuleClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public DatasetModuleClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public DatasetModuleClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Lists all dataset modules.
   *
   * @return list of {@link DatasetModuleMeta}s.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<DatasetModuleMeta> list(NamespaceId namespace)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(namespace, "data/modules");
    return ObjectResponse.fromJsonBody(restClient.execute(HttpMethod.GET, url, config.getAccessToken()),
                                       new TypeToken<List<DatasetModuleMeta>>() { }).getResponseObject();
  }

  /**
   * Adds a new dataset module.
   *
   * @param module the new dataset module
   * @param className name of the dataset module class within the moduleJarFile
   * @param moduleJarFile Jar file containing the dataset module class and dependencies
   * @throws BadRequestException if the moduleJarFile does not exist
   * @throws AlreadyExistsException if a dataset module with the same name already exists
   * @throws IOException if a network error occurred
   */
  public void add(DatasetModuleId module, String className, File moduleJarFile)
    throws BadRequestException, AlreadyExistsException, IOException, UnauthenticatedException {

    URL url = config.resolveNamespacedURLV3(module.getParent(), String.format("data/modules/%s", module.getModule()));
    Map<String, String> headers = ImmutableMap.of("X-Class-Name", className);
    HttpRequest request = HttpRequest.put(url).addHeaders(headers).withBody(moduleJarFile).build();

    HttpResponse response = restClient.upload(request, config.getAccessToken(),
                                              HttpURLConnection.HTTP_BAD_REQUEST,
                                              HttpURLConnection.HTTP_CONFLICT);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(String.format("Module jar file does not exist: %s", moduleJarFile));
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new DatasetModuleAlreadyExistsException(module);
    }
  }

  /**
   * Deletes a dataset module.
   *
   * @param module the dataset module to delete
   * @throws DatasetModuleCannotBeDeletedException if the dataset module cannot be deleted,
   * usually due to other dataset modules or dataset instances using the dataset module
   * @throws DatasetModuleNotFoundException if the dataset module with the specified name was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void delete(DatasetModuleId module)
    throws DatasetModuleCannotBeDeletedException, DatasetModuleNotFoundException,
    IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(module.getParent(), String.format("data/modules/%s", module.getModule()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_CONFLICT,
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new DatasetModuleCannotBeDeletedException(module);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new DatasetModuleNotFoundException(module);
    }
  }

  /**
   * Checks if a dataset module exists.
   *
   * @param module the dataset module to check
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public boolean exists(DatasetModuleId module) throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(module.getParent(), String.format("data/modules/%s", module.getModule()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    return response.getResponseCode() != HttpURLConnection.HTTP_NOT_FOUND;
  }

  /**
   * Deletes all dataset modules in a namespace.
   *
   * @throws DatasetModuleCannotBeDeletedException if one of the dataset modules cannot be deleted,
   * usually due to existing dataset instances using the dataset module
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void deleteAll(NamespaceId namespace)
    throws DatasetModuleCannotBeDeletedException, IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(namespace, "data/modules");
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_CONFLICT);
    if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      // TODO: exception for all modules
      throw new DatasetModuleCannotBeDeletedException(null);
    }
  }

  /**
   * Gets information about a dataset module.
   *
   * @param module the dataset module
   * @return {@link DatasetModuleMeta} of the dataset module
   * @throws DatasetModuleNotFoundException if the dataset module with the specified name was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public DatasetModuleMeta get(DatasetModuleId module)
    throws DatasetModuleNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(module.getParent(), String.format("data/modules/%s", module.getModule()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new DatasetModuleNotFoundException(module);
    }

    return ObjectResponse.fromJsonBody(response, DatasetModuleMeta.class).getResponseObject();
  }
}
