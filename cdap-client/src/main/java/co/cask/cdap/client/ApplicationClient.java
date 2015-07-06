/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
 * Provides ways to interact with CDAP applications.
 */
public class ApplicationClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ApplicationClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public ApplicationClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Lists all applications currently deployed.
   *
   * @return list of {@link ApplicationRecord}s.
   * @throws IOException
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<ApplicationRecord> list() throws IOException, UnauthorizedException {
    HttpResponse response = restClient.execute(HttpMethod.GET,
                                               config.resolveNamespacedURLV3("apps"),
                                               config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ApplicationRecord>>() { }).getResponseObject();
  }

  /**
   * Deletes an application.
   *
   * @param appId ID of the application to delete
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void delete(String appId) throws ApplicationNotFoundException, IOException, UnauthorizedException {
    Id.Application app = Id.Application.from(config.getNamespace(), appId);
    HttpResponse response = restClient.execute(HttpMethod.DELETE,
                                               config.resolveNamespacedURLV3("apps/" + app.getId()),
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(app);
    }
  }

  /**
   * Deletes all applications.
   *
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void deleteAll() throws IOException, UnauthorizedException {
    restClient.execute(HttpMethod.DELETE, config.resolveNamespacedURLV3("apps"), config.getAccessToken());
  }

  /**
   * Checks if an application exists.
   *
   * @param appId ID of the application to check
   * @return true if the application exists
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public boolean exists(String appId) throws IOException, UnauthorizedException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveNamespacedURLV3("apps/" + appId),
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    return response.getResponseCode() != HttpURLConnection.HTTP_NOT_FOUND;
  }

  /**
   * Waits for an application to be deployed.
   *
   * @param appId ID of the application to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the application was not yet deployed before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeployed(final String appId, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(appId);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Waits for an application to be deleted.
   *
   * @param appId ID of the application to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the application was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeleted(final String appId, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthorizedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(false, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(appId);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthorizedException.class);
    }
  }

  /**
   * Deploys an application.
   *
   * @param jarFile jar file of the application to deploy
   * @throws IOException if a network error occurred
   */
  public void deploy(File jarFile) throws IOException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3("apps");
    Map<String, String> headers = ImmutableMap.of("X-Archive-Name", jarFile.getName());

    HttpRequest request = HttpRequest.post(url).addHeaders(headers).withBody(jarFile).build();
    restClient.upload(request, config.getAccessToken());
  }

  /**
   * Lists all programs of some type.
   *
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<ProgramRecord> listAllPrograms(ProgramType programType) throws IOException, UnauthorizedException {
    Preconditions.checkArgument(programType.isListable());

    String path = programType.getCategoryName();
    URL url = config.resolveNamespacedURLV3(path);
    HttpRequest request = HttpRequest.get(url).build();

    ObjectResponse<List<ProgramRecord>> response = ObjectResponse.fromJsonBody(
      restClient.execute(request, config.getAccessToken()), new TypeToken<List<ProgramRecord>>() { });

    return response.getResponseObject();
  }

  /**
   * Lists all programs.
   *
   * @return list of {@link ProgramRecord}s
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public Map<ProgramType, List<ProgramRecord>> listAllPrograms() throws IOException, UnauthorizedException {

    ImmutableMap.Builder<ProgramType, List<ProgramRecord>> allPrograms = ImmutableMap.builder();
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isListable()) {
        List<ProgramRecord> programRecords = Lists.newArrayList();
        programRecords.addAll(listAllPrograms(programType));
        allPrograms.put(programType, programRecords);
      }
    }
    return allPrograms.build();
  }

  /**
   * Lists programs of some type belonging to an application.
   *
   * @param appId ID of the application
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<ProgramRecord> listPrograms(String appId, ProgramType programType)
    throws ApplicationNotFoundException, IOException, UnauthorizedException {

    Preconditions.checkArgument(programType.isListable());

    List<ProgramRecord> programs = Lists.newArrayList();
    for (ProgramRecord program : listPrograms(appId)) {
      if (programType.equals(program.getType())) {
        programs.add(program);
      }
    }
    return programs;
  }

  /**
   * Lists programs of some type belonging to an application.
   *
   * @param appId ID of the application
   * @return Map of {@link ProgramType} to list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public Map<ProgramType, List<ProgramRecord>> listProgramsByType(String appId)
    throws ApplicationNotFoundException, IOException, UnauthorizedException {

    Map<ProgramType, List<ProgramRecord>> result = Maps.newHashMap();
    for (ProgramType type : ProgramType.values()) {
      result.put(type, Lists.<ProgramRecord>newArrayList());
    }
    for (ProgramRecord program : listPrograms(appId)) {
      result.get(program.getType()).add(program);
    }
    return result;
  }

  /**
   * Lists programs belonging to an application.
   *
   * @param appId ID of the application
   * @return List of all {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<ProgramRecord> listPrograms(String appId)
    throws ApplicationNotFoundException, IOException, UnauthorizedException {

    String path = String.format("apps/%s", appId);
    URL url = config.resolveNamespacedURLV3(path);
    HttpRequest request = HttpRequest.get(url).build();

    ObjectResponse<ApplicationDetail> response = ObjectResponse.fromJsonBody(
      restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND),
      new TypeToken<ApplicationDetail>() { });

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(Id.Application.from(config.getNamespace(), appId));
    }

    return response.getResponseObject().getPrograms();
  }
}
