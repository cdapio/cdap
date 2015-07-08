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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.AdapterNotFoundException;
import co.cask.cdap.common.ApplicationTemplateNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterDetail;
import co.cask.cdap.proto.AdapterStatus;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramId;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Adapters.
 */
@Beta
public class AdapterClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public AdapterClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public AdapterClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Lists all adapters.
   *
   * @return list of {@link AdapterDetail}.
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #list(Id.Namespace)} instead.
   */
  @Deprecated
  public List<AdapterDetail> list() throws IOException, UnauthorizedException {
    return list(config.getNamespace());
  }

  /**
   * Gets an adapter.
   *
   * @return an {@link AdapterDetail}.
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #get(Id.Adapter)} instead.
   */
  @Deprecated
  public AdapterDetail get(String adapterName) throws AdapterNotFoundException, IOException, UnauthorizedException {
    return get(Id.Adapter.from(config.getNamespace(), adapterName));
  }

  /**
   * Creates an adapter.
   *
   * @param adapterName name of the adapter to create
   * @param adapterSpec properties of the adapter to create
   * @throws ApplicationTemplateNotFoundException if the desired adapter type was not found
   * @throws BadRequestException if the provided {@link AdapterConfig} was bad
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #create(Id.Adapter, AdapterConfig)} instead.
   */
  @Deprecated
  public void create(String adapterName, AdapterConfig adapterSpec)
    throws ApplicationTemplateNotFoundException, BadRequestException, IOException, UnauthorizedException {

    create(Id.Adapter.from(config.getNamespace(), adapterName), adapterSpec);
  }

  /**
   * Deletes an adapter.
   *
   * @param adapterName Name of the adapter to delete
   * @throws AdapterNotFoundException if the adapter with the specified name could not be found
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #delete(Id.Adapter)} instead.
   */
  @Deprecated
  public void delete(String adapterName) throws AdapterNotFoundException, IOException, UnauthorizedException {
    delete(Id.Adapter.from(config.getNamespace(), adapterName));
  }

  /**
   * Checks if a adapter exists.
   *
   * @param adapterName Name of the adapter to check
   * @return true if the adapter exists
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #exists(Id.Adapter)} instead.
   */
  @Deprecated
  public boolean exists(String adapterName) throws IOException, UnauthorizedException {
    return exists(Id.Adapter.from(config.getNamespace(), adapterName));
  }

  /**
   * Waits for an adapter to exist.
   *
   * @param adapterName Name of the adapter to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the adapter was not yet existent before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   *
   * @deprecated As of 3.1, use {@link #waitForExists(Id.Adapter, long, TimeUnit)} instead.
   */
  @Deprecated
  public void waitForExists(final String adapterName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    waitForExists(Id.Adapter.from(config.getNamespace(), adapterName), timeout, timeoutUnit);
  }

  /**
   * Waits for an adapter to be deleted.
   *
   * @param adapterName Name of the adapter to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the adapter was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   *
   * @deprecated As of 3.1, use {@link #waitForDeleted(Id.Adapter, long, TimeUnit)} instead.
   */
  @Deprecated
  public void waitForDeleted(final String adapterName, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    waitForDeleted(Id.Adapter.from(config.getNamespace(), adapterName), timeout, timeoutUnit);
  }

  /**
   * Starts an adapter.
   *
   * @param adapterName the name of the adapter to start
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws AdapterNotFoundException if the adapter is not found
   *
   * @deprecated As of 3.1, use {@link #start(Id.Adapter)} instead.
   */
  @Deprecated
  public void start(String adapterName) throws IOException, UnauthorizedException, AdapterNotFoundException {
    start(Id.Adapter.from(config.getNamespace(), adapterName));
  }

  /**
   * Stops an adapter.
   *
   * @param adapterName the name of the adapter to stop
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws AdapterNotFoundException if the adapter is not found
   *
   * @deprecated As of 3.1, use {@link #stop(Id.Adapter)} instead.
   */
  @Deprecated
  public void stop(String adapterName) throws IOException, UnauthorizedException, AdapterNotFoundException {
    stop(Id.Adapter.from(config.getNamespace(), adapterName));
  }

  /**
   * Waits for an adapter to have a certain status.
   *
   * @param adapterName name of the adapter to check
   * @param status the status to wait for
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the adapter was not yet existent before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   *
   * @deprecated As of 3.1, use {@link #waitForStatus(Id.Adapter, AdapterStatus, long, TimeUnit)} instead.
   */
  @Deprecated
  public void waitForStatus(final String adapterName, final AdapterStatus status, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    waitForStatus(Id.Adapter.from(config.getNamespace(), adapterName), status, timeout, timeoutUnit);
  }

  /**
   * @deprecated As of 3.1, use {@link #getLogs(Id.Adapter)} instead.
   */
  @Deprecated
  public String getLogs(String adapterName)
    throws AdapterNotFoundException, UnauthorizedException, IOException {
    return getLogs(Id.Adapter.from(config.getNamespace(), adapterName));
  }

  /**
   * @deprecated As of 3.1, use {@link #getLogs(Id.Adapter, Long, Long)} instead.
   */
  @Deprecated
  public String getLogs(String adapterName, @Nullable Long start, @Nullable Long stop)
    throws AdapterNotFoundException, UnauthorizedException, IOException {
    return getLogs(Id.Adapter.from(config.getNamespace(), adapterName), start, stop);
  }

  /**
   * @deprecated As of 3.1, use {@link #getLogs(Id.Adapter, Long, Long, Boolean, String)} instead.
   */
  @Deprecated
  public String getLogs(String adapterName, @Nullable Long start, @Nullable Long stop,
                        @Nullable Boolean escape, @Nullable String filter)
    throws IOException, AdapterNotFoundException, UnauthorizedException {
    return getLogs(Id.Adapter.from(config.getNamespace(), adapterName), start, stop, escape, filter);
  }

  /**
   * @deprecated As of 3.1, use {@link #getStatus(Id.Adapter)} instead.
   */
  @Deprecated
  public AdapterStatus getStatus(String adapterName)
    throws IOException, UnauthorizedException, AdapterNotFoundException {

    return getStatus(Id.Adapter.from(config.getNamespace(), adapterName));
  }

  /**
   * @deprecated As of 3.1, use {@link #getRuns(Id.Adapter, ProgramRunStatus, long, long, Integer)} instead.
   */
  @Deprecated
  public List<RunRecord> getRuns(String adapterName, ProgramRunStatus status, long startTs, long endTs,
                                 @Nullable Integer resultLimit)
    throws IOException, UnauthorizedException, AdapterNotFoundException {

    return getRuns(Id.Adapter.from(config.getNamespace(), adapterName), status, startTs, endTs, resultLimit);
  }

  /**
   * Lists all adapters.
   *
   * @return list of {@link AdapterDetail}.
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<AdapterDetail> list(Id.Namespace namespace) throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(namespace, "adapters");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<AdapterDetail>>() { }, GSON)
      .getResponseObject();
  }

  /**
   * Gets an adapter.
   *
   * @return an {@link AdapterDetail}.
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public AdapterDetail get(Id.Adapter adapter) throws AdapterNotFoundException, IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(), "adapters/" + adapter.getId());
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapter);
    }
    return ObjectResponse.fromJsonBody(response, AdapterDetail.class, GSON).getResponseObject();
  }

  /**
   * Creates an adapter.
   *
   * @param adapter name of the adapter to create
   * @param adapterSpec properties of the adapter to create
   * @throws ApplicationTemplateNotFoundException if the desired adapter type was not found
   * @throws BadRequestException if the provided {@link AdapterConfig} was bad
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void create(Id.Adapter adapter, AdapterConfig adapterSpec)
    throws ApplicationTemplateNotFoundException, BadRequestException, IOException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(), String.format("adapters/%s", adapter.getId()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(adapterSpec)).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationTemplateNotFoundException(Id.ApplicationTemplate.from(adapterSpec.getTemplate()));
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
  }

  /**
   * Deletes an adapter.
   *
   * @param adapter the adapter to delete
   * @throws AdapterNotFoundException if the adapter with the specified name could not be found
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void delete(Id.Adapter adapter) throws AdapterNotFoundException, IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(), String.format("adapters/%s", adapter.getId()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapter);
    }
  }

  /**
   * Checks if a adapter exists.
   *
   * @param adapter the adapter to check
   * @return true if the adapter exists
   * @throws java.io.IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public boolean exists(Id.Adapter adapter) throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(), String.format("adapters/%s", adapter.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    return response.getResponseCode() != HttpURLConnection.HTTP_NOT_FOUND;
  }

  /**
   * Waits for an adapter to exist.
   *
   * @param adapter the adapter to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the adapter was not yet existent before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForExists(final Id.Adapter adapter, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(adapter);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Waits for an adapter to be deleted.
   *
   * @param adapter the adapter to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the adapter was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeleted(final Id.Adapter adapter, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(false, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(adapter);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Starts an adapter.
   *
   * @param adapter the adapter to start
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws AdapterNotFoundException if the adapter is not found
   */
  public void start(Id.Adapter adapter) throws IOException, UnauthorizedException, AdapterNotFoundException {
    URL url = config.resolveNamespacedURLV3(
      adapter.getNamespace(), String.format("adapters/%s/start", adapter.getId()));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapter);
    }
  }

  /**
   * Stops an adapter.
   *
   * @param adapter the adapter to stop
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws AdapterNotFoundException if the adapter is not found
   */
  public void stop(Id.Adapter adapter) throws IOException, UnauthorizedException, AdapterNotFoundException {
    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(),
                                            String.format("adapters/%s/stop", adapter.getId()));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapter);
    }
  }

  /**
   * Waits for an adapter to have a certain status.
   *
   * @param adapter the adapter to check
   * @param status the status to wait for
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the adapter was not yet existent before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForStatus(final Id.Adapter adapter, final AdapterStatus status, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return status.equals(getStatus(adapter));
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  public String getLogs(Id.Adapter adapter)
    throws AdapterNotFoundException, UnauthorizedException, IOException {
    return getLogs(adapter, null, null, null, null);
  }

  public String getLogs(Id.Adapter adapter, @Nullable Long start, @Nullable Long stop)
    throws AdapterNotFoundException, UnauthorizedException, IOException {
    return getLogs(adapter, start, stop, null, null);
  }

  public String getLogs(Id.Adapter adapter, @Nullable Long start, @Nullable Long stop,
                        @Nullable Boolean escape, @Nullable String filter)
    throws IOException, AdapterNotFoundException, UnauthorizedException {

    Multimap<String, String> queryParams = HashMultimap.create();
    queryParams.put("adapterid", adapter.getId());

    if (start != null) {
      queryParams.put("start", Long.toString(start));
    }
    if (stop != null) {
      queryParams.put("stop", Long.toString(stop));
    }
    if (escape != null) {
      queryParams.put("escape", Boolean.toString(escape));
    }
    if (filter != null) {
      queryParams.put("filter", filter);
    }

    String queryString = Joiner.on("&").join(queryParams.entries());
    AdapterDetail adapterDetail = get(adapter);
    ProgramId program = adapterDetail.getProgram();
    // TODO: currently doesn't work for workflows since getting workflow logs is not implemented yet

    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(),
                                            String.format("apps/%s/%s/%s/logs?%s",
                                                          program.getApplication(),
                                                          program.getType().getCategoryName(),
                                                          program.getId(), queryString));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapter);
    }

    return response.getResponseBodyAsString();
  }

  public AdapterStatus getStatus(Id.Adapter adapter)
    throws IOException, UnauthorizedException, AdapterNotFoundException {

    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(),
                                            String.format("adapters/%s/status", adapter.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapter);
    }

    Map<String, String> statusMap = ObjectResponse.<Map<String, String>>fromJsonBody(
      response, new TypeToken<Map<String, String>>() { }.getType()).getResponseObject();

    return AdapterStatus.valueOf(statusMap.get("status"));
  }

  public List<RunRecord> getRuns(Id.Adapter adapter, ProgramRunStatus status, long startTs, long endTs,
                                 @Nullable Integer resultLimit)
    throws IOException, UnauthorizedException, AdapterNotFoundException {

    String query = "?status=" + status + "&start=" + startTs + "&end=" + endTs +
      (resultLimit == null ? "" : "&resultLimit=" + resultLimit);

    URL url = config.resolveNamespacedURLV3(adapter.getNamespace(),
                                            String.format("adapters/%s/runs" + query, adapter.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new AdapterNotFoundException(adapter);
    }

    return ObjectResponse.<List<RunRecord>>fromJsonBody(
      response, new TypeToken<List<RunRecord>>() { }.getType()).getResponseObject();
  }
}
