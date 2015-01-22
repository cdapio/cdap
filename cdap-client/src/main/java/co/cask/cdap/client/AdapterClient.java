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
import co.cask.cdap.client.exception.AdapterNotFoundException;
import co.cask.cdap.client.exception.AdapterTypeNotFoundException;
import co.cask.cdap.client.exception.BadRequestException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Adapters.
 */
public class AdapterClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public AdapterClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Lists all adapters.
   *
   * @param namespace the namespace to use
   * @return list of {@link AdapterSpecification}.
   * @throws java.io.IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public List<AdapterSpecification> list(String namespace) throws IOException, UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("v3", namespace, "adapters");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<AdapterSpecification>>() { })
      .getResponseObject();
  }

  /**
   * Gets an adapter.
   *
   * @param namespace the namespace to use
   * @return an {@link AdapterSpecification}.
   * @throws java.io.IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public AdapterSpecification get(String namespace, String adapterName)
    throws AdapterNotFoundException, IOException, UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("v3", namespace, "adapters/" + adapterName);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapterName);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<AdapterSpecification>() { }).getResponseObject();
  }

  /**
   * Creates an adapter.
   *
   * @param namespace the namespace to use
   * @param adapterName name of the adapter to create
   * @param adapterConfig properties of the adapter to create
   * @throws AdapterTypeNotFoundException if the desired adapter type was not found
   * @throws BadRequestException if the provided {@link AdapterConfig} was bad
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void create(String namespace, String adapterName, AdapterConfig adapterConfig)
    throws AdapterTypeNotFoundException, BadRequestException, IOException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL("v3", namespace, String.format("adapters/%s", adapterName));
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(adapterConfig)).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterTypeNotFoundException(adapterConfig.type);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseMessage());
    }
  }

  /**
   * Deletes an adapter.
   *
   * @param namespace the namespace to use
   * @param adapterName Name of the adapter to delete
   * @throws AdapterNotFoundException if the dataset with the specified name could not be found
   * @throws java.io.IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void delete(String namespace, String adapterName) throws AdapterNotFoundException, IOException,
    UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("v3", namespace, String.format("adapters/%s", adapterName));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapterName);
    }
  }

  /**
   * Checks if a dataset exists.
   *
   * @param namespace the namespace to use
   * @param adapterName Name of the adapter to check
   * @return true if the dataset exists
   * @throws java.io.IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public boolean exists(String namespace, String adapterName) throws IOException, UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("v3", namespace, String.format("adapters/%s", adapterName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    return response.getResponseCode() != HttpURLConnection.HTTP_NOT_FOUND;
  }

  /**
   * Waits for an adapter to exist.
   *
   * @param namespace the namespace to use
   * @param adapterName Name of the dataset to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the dataset was not yet existent before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForExists(final String namespace, final String adapterName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnAuthorizedAccessTokenException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(namespace, adapterName);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnAuthorizedAccessTokenException.class);
    }
  }

  /**
   * Waits for an adapter to be deleted.
   *
   * @param adapterName Name of the dataset to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the dataset was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeleted(final String namespace, final String adapterName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnAuthorizedAccessTokenException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(false, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(namespace, adapterName);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnAuthorizedAccessTokenException.class);
    }
  }
}
