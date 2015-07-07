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
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.DatasetModuleAlreadyExistsException;
import co.cask.cdap.common.DatasetModuleCannotBeDeletedException;
import co.cask.cdap.common.DatasetModuleNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Dataset modules.
 */
public class DatasetModuleClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public DatasetModuleClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public DatasetModuleClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Lists all dataset modules.
   *
   * @return list of {@link DatasetModuleMeta}s.
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<DatasetModuleMeta> list() throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3("data/modules");
    return ObjectResponse.fromJsonBody(restClient.execute(HttpMethod.GET, url, config.getAccessToken()),
                                       new TypeToken<List<DatasetModuleMeta>>() { }).getResponseObject();
  }

  /**
   * Adds a new dataset module.
   *
   * @param moduleName name of the new dataset module
   * @param className name of the dataset module class within the moduleJarFile
   * @param moduleJarFile Jar file containing the dataset module class and dependencies
   * @throws BadRequestException if the moduleJarFile does not exist
   * @throws AlreadyExistsException if a dataset module with the same name already exists
   * @throws IOException if a network error occurred
   */
  public void add(String moduleName, String className, File moduleJarFile)
    throws BadRequestException, AlreadyExistsException, IOException, UnauthorizedException {

    Id.DatasetModule module = Id.DatasetModule.from(config.getNamespace(), moduleName);
    URL url = config.resolveNamespacedURLV3(String.format("data/modules/%s", moduleName));
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
   * @param moduleName name of the dataset module to delete
   * @throws DatasetModuleCannotBeDeletedException if the dataset module cannot be deleted,
   * usually due to other dataset modules or dataset instances using the dataset module
   * @throws DatasetModuleNotFoundException if the dataset module with the specified name was not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void delete(String moduleName)
    throws DatasetModuleCannotBeDeletedException, DatasetModuleNotFoundException, IOException, UnauthorizedException {

    Id.DatasetModule module = Id.DatasetModule.from(config.getNamespace(), moduleName);
    URL url = config.resolveNamespacedURLV3(String.format("data/modules/%s", moduleName));
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
   * @param moduleName name of the dataset module to check
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public boolean exists(String moduleName) throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(String.format("data/modules/%s", moduleName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    return response.getResponseCode() != HttpURLConnection.HTTP_NOT_FOUND;
  }

  /**
   * Waits for a dataset module to exist.
   *
   * @param moduleName Name of the dataset module to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the dataset module was not yet existent before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForExists(final String moduleName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(moduleName);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Waits for a dataset module to be deleted.
   *
   * @param moduleName Name of the dataset module to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the dataset module was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeleted(final String moduleName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(false, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(moduleName);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Deletes all dataset modules.
   *
   * @throws DatasetModuleCannotBeDeletedException if one of the dataset modules cannot be deleted,
   * usually due to existing dataset instances using the dataset module
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void deleteAll() throws DatasetModuleCannotBeDeletedException, IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3("data/modules");
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
   * @param moduleName name of the dataset module
   * @return {@link DatasetModuleMeta} of the dataset module
   * @throws DatasetModuleNotFoundException if the dataset module with the specified name was not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public DatasetModuleMeta get(String moduleName)
    throws DatasetModuleNotFoundException, IOException, UnauthorizedException {

    Id.DatasetModule module = Id.DatasetModule.from(config.getNamespace(), moduleName);
    URL url = config.resolveNamespacedURLV3(String.format("data/modules/%s", moduleName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new DatasetModuleNotFoundException(module);
    }

    return ObjectResponse.fromJsonBody(response, DatasetModuleMeta.class).getResponseObject();
  }
}
