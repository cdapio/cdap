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
import co.cask.cdap.common.DatasetAlreadyExistsException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

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
 * Provides ways to interact with CDAP Datasets.
 */
public class DatasetClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public DatasetClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public DatasetClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Lists all datasets.
   *
   * @return list of {@link DatasetSpecificationSummary}.
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<DatasetSpecificationSummary> list() throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3("data/datasets");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response,
                                       new TypeToken<List<DatasetSpecificationSummary>>() { }).getResponseObject();
  }


  /**
   * Gets information about a dataset.
   *
   * @return a {@link DatasetSpecificationSummary}.
   * @throws NotFoundException if the dataset is not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public DatasetMeta get(String datasetName)
    throws IOException, UnauthorizedException, NotFoundException {

    URL url = config.resolveNamespacedURLV3(String.format("data/datasets/%s", datasetName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      Id.DatasetInstance instance = Id.DatasetInstance.from(config.getNamespace(), datasetName);
      throw new NotFoundException(instance);
    }
    return ObjectResponse.fromJsonBody(response, DatasetMeta.class).getResponseObject();
  }

  /**
   * Creates a dataset.
   *
   * @param datasetName name of the dataset to create
   * @param properties properties of the dataset to create
   * @throws DatasetTypeNotFoundException if the desired dataset type was not found
   * @throws DatasetAlreadyExistsException if a dataset by the same name already exists
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void create(String datasetName, DatasetInstanceConfiguration properties)
    throws DatasetTypeNotFoundException, DatasetAlreadyExistsException, IOException, UnauthorizedException {

    Id.DatasetInstance instance = Id.DatasetInstance.from(config.getNamespace(), datasetName);
    URL url = config.resolveNamespacedURLV3(String.format("data/datasets/%s", datasetName));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(properties)).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_CONFLICT);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new DatasetTypeNotFoundException(Id.DatasetType.from(config.getNamespace(), properties.getTypeName()));
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new DatasetAlreadyExistsException(instance);
    }
  }

  /**
   * Creates a dataset.
   *
   * @param datasetName Name of the dataset to create
   * @param typeName Type of dataset to create
   * @throws DatasetTypeNotFoundException if the desired dataset type was not found
   * @throws DatasetAlreadyExistsException if a dataset by the same name already exists
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void create(String datasetName, String typeName)
    throws DatasetTypeNotFoundException, DatasetAlreadyExistsException, IOException, UnauthorizedException {
    create(datasetName, new DatasetInstanceConfiguration(typeName, ImmutableMap.<String, String>of()));
  }

  /**
   * Updates the properties of a dataset.
   *
   * @param datasetName Name of the dataset to update
   * @param properties Properties to set
   * @throws NotFoundException if the dataset is not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void update(String datasetName, Map<String, String> properties)
    throws NotFoundException, IOException, UnauthorizedException {
    Id.DatasetInstance instance = Id.DatasetInstance.from(config.getNamespace(), datasetName);
    URL url = config.resolveNamespacedURLV3(String.format("data/datasets/%s/properties", datasetName));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(properties)).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_CONFLICT);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(instance);
    }
  }

  /**
   * Updates the existing properties of a dataset.
   *
   * @param datasetName Name of the dataset to update
   * @param properties Properties to set
   * @throws NotFoundException if the dataset is not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void updateExisting(String datasetName, Map<String, String> properties)
    throws NotFoundException, IOException, UnauthorizedException {

    DatasetMeta meta = get(datasetName);
    Map<String, String> existingProperties = meta.getSpec().getProperties();

    Map<String, String> resolvedProperties = Maps.newHashMap();
    resolvedProperties.putAll(existingProperties);
    resolvedProperties.putAll(properties);

    update(datasetName, resolvedProperties);
  }

  /**
   * Deletes a dataset.
   *
   * @param datasetName Name of the dataset to delete
   * @throws DatasetNotFoundException if the dataset with the specified name could not be found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void delete(String datasetName) throws DatasetNotFoundException, IOException, UnauthorizedException {
    Id.DatasetInstance instance = Id.DatasetInstance.from(config.getNamespace(), datasetName);
    URL url = config.resolveNamespacedURLV3(String.format("data/datasets/%s", datasetName));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new DatasetNotFoundException(instance);
    }
  }

  /**
   * Checks if a dataset exists.
   *
   * @param datasetName Name of the dataset to check
   * @return true if the dataset exists
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public boolean exists(String datasetName) throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(String.format("data/datasets/%s", datasetName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    return response.getResponseCode() != HttpURLConnection.HTTP_NOT_FOUND;
  }

  /**
   * Waits for a dataset to exist.
   *
   * @param datasetName Name of the dataset to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the dataset was not yet existent before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForExists(final String datasetName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(datasetName);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Waits for a dataset to be deleted.
   *
   * @param datasetName Name of the dataset to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the dataset was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeleted(final String datasetName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(false, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(datasetName);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Truncates a dataset. This will clear all data belonging to the dataset.
   *
   * @param datasetName Name of the dataset to truncate
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void truncate(String datasetName) throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(String.format("data/datasets/%s/admin/truncate", datasetName));
    restClient.execute(HttpMethod.POST, url, config.getAccessToken());
  }
}
