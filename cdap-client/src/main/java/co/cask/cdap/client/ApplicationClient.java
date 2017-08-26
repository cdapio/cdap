/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.PluginInstanceDetail;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Provides ways to interact with CDAP applications.
 */
@Beta
public class ApplicationClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ApplicationClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public ApplicationClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Lists all applications currently deployed.
   *
   * @param namespace the namespace to list applications from
   * @return list of {@link ApplicationRecord ApplicationRecords}.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #list(NamespaceId)} instead
   */
  @Deprecated
  public List<ApplicationRecord> list(Id.Namespace namespace)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return list(namespace.toEntityId());
  }

  /**
   * Lists all applications currently deployed.
   *
   * @param namespace the namespace to list applications from
   * @return list of {@link ApplicationRecord ApplicationRecords}.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<ApplicationRecord> list(NamespaceId namespace)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    HttpResponse response = restClient.execute(HttpMethod.GET,
                                               config.resolveNamespacedURLV3(namespace, "apps"),
                                               config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ApplicationRecord>>() { }).getResponseObject();
  }

  /**
   * Lists all applications currently deployed, optionally filtering to only include applications that use the
   * specified artifact name and version.
   *
   * @param namespace the namespace to list applications from
   * @param artifactName the name of the artifact to filter by. If null, no filtering will be done.
   * @param artifactVersion the version of the artifact to filter by. If null, no filtering will be done.
   * @return list of {@link ApplicationRecord ApplicationRecords}.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #list(NamespaceId, String, String)}.
   */
  @Deprecated
  public List<ApplicationRecord> list(
    Id.Namespace namespace, @Nullable String artifactName,
    @Nullable String artifactVersion) throws IOException, UnauthenticatedException, UnauthorizedException {
    return list(namespace.toEntityId(), artifactName, artifactVersion);
  }

  /**
   * Lists all applications currently deployed, optionally filtering to only include applications that use the
   * specified artifact name and version.
   *
   * @param namespace the namespace to list applications from
   * @param artifactName the name of the artifact to filter by. If null, no filtering will be done.
   * @param artifactVersion the version of the artifact to filter by. If null, no filtering will be done.
   * @return list of {@link ApplicationRecord ApplicationRecords}.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<ApplicationRecord> list(
    NamespaceId namespace, @Nullable String artifactName,
    @Nullable String artifactVersion) throws IOException, UnauthenticatedException, UnauthorizedException {
    Set<String> names = new HashSet<>();
    if (artifactName != null) {
      names.add(artifactName);
    }
    return list(namespace, names, artifactVersion);
  }

  /**
   * Lists all applications currently deployed, optionally filtering to only include applications that use one of
   * the specified artifact names and the specified artifact version.
   *
   * @param namespace the namespace to list applications from
   * @param artifactNames the set of artifact names to allow. If empty, no filtering will be done.
   * @param artifactVersion the version of the artifact to filter by. If null, no filtering will be done.
   * @return list of {@link ApplicationRecord ApplicationRecords}.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #list(NamespaceId, Set, String)} instead.
   */
  @Deprecated
  public List<ApplicationRecord> list(
    Id.Namespace namespace, Set<String> artifactNames, @Nullable String artifactVersion)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return list(namespace.toEntityId(), artifactNames, artifactVersion);
  }

  /**
   * Lists all applications currently deployed, optionally filtering to only include applications that use one of
   * the specified artifact names and the specified artifact version.
   *
   * @param namespace the namespace to list applications from
   * @param artifactNames the set of artifact names to allow. If empty, no filtering will be done.
   * @param artifactVersion the version of the artifact to filter by. If null, no filtering will be done.
   * @return list of {@link ApplicationRecord ApplicationRecords}.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<ApplicationRecord> list(
    NamespaceId namespace, Set<String> artifactNames, @Nullable String artifactVersion)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    if (artifactNames.isEmpty() && artifactVersion == null) {
      return list(namespace);
    }

    String path;
    if (!artifactNames.isEmpty() && artifactVersion != null) {
      path = String.format("apps?artifactName=%s&artifactVersion=%s",
                           Joiner.on(',').join(artifactNames), artifactVersion);
    } else if (!artifactNames.isEmpty()) {
      path = "apps?artifactName=" + Joiner.on(',').join(artifactNames);
    } else {
      path = "apps?artifactVersion=" + artifactVersion;
    }

    HttpResponse response = restClient.execute(HttpMethod.GET,
                                               config.resolveNamespacedURLV3(namespace, path),
                                               config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ApplicationRecord>>() { }).getResponseObject();
  }

  /**
   * Lists all applications currently deployed, optionally filtering to only include applications that use one of
   * the specified artifact names and the specified artifact version.
   *
   * @param namespace the namespace to list application versions from
   * @param appName the application name to list versions for
   * @return list of {@link String} application versions.
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<String> listAppVersions(NamespaceId namespace, String appName)
    throws IOException, UnauthenticatedException, UnauthorizedException, ApplicationNotFoundException {
    String path = String.format("apps/%s/versions", appName);
    URL url = config.resolveNamespacedURLV3(namespace, path);
    HttpRequest request = HttpRequest.get(url).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(namespace.app(appName));
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<String>>() { }).getResponseObject();
  }

  /**
   * Get details about the specified application.
   *
   * @param appId the id of the application to get
   * @return details about the specified application
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #get(ApplicationId) ()} instead
   */
  @Deprecated
  public ApplicationDetail get(Id.Application appId)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return get(appId.toEntityId());
  }

  /**
   * Get details about the specified application.
   *
   * @param appId the id of the application to get
   * @return details about the specified application
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public ApplicationDetail get(ApplicationId appId)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("apps/%s/versions/%s", appId.getApplication(), appId.getVersion());
    HttpResponse response = restClient.execute(HttpMethod.GET,
                                               config.resolveNamespacedURLV3(appId.getParent(), path),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(appId);
    }
    return ObjectResponse.fromJsonBody(response, ApplicationDetail.class).getResponseObject();
  }

  /**
   * Get plugins in the specified application.
   *
   * @param appId the id of the application to get
   * @return list of plugins in the application
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<PluginInstanceDetail> getPlugins(ApplicationId appId)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    HttpResponse response = restClient.execute(HttpMethod.GET,
                                               config.resolveNamespacedURLV3(
                                                 appId.getParent(),
                                                 "apps/" + appId.getApplication() + "/plugins"),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(appId);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<PluginInstanceDetail>>() { }).getResponseObject();
  }

  /**
   * Deletes an application.
   *
   * @param app the application to delete
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #delete(ApplicationId) ()} instead
   */
  @Deprecated
  public void delete(Id.Application app)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    delete(app.toEntityId());
  }

  /**
   * Deletes an application.
   *
   * @param app the application to delete
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void delete(ApplicationId app)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    String path = String.format("apps/%s/versions/%s", app.getApplication(), app.getVersion());
    HttpResponse response = restClient.execute(HttpMethod.DELETE,
                                               config.resolveNamespacedURLV3(app.getParent(), path),
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(app);
    }
  }

  /**
   * Deletes all applications in a namespace.
   *
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #deleteAll(NamespaceId)} instead.
   */
  @Deprecated
  public void deleteAll(Id.Namespace namespace) throws IOException, UnauthenticatedException, UnauthorizedException {
    deleteAll(namespace.toEntityId());
  }

  /**
   * Deletes all applications in a namespace.
   *
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void deleteAll(NamespaceId namespace) throws IOException, UnauthenticatedException, UnauthorizedException {
    restClient.execute(HttpMethod.DELETE, config.resolveNamespacedURLV3(namespace, "apps"), config.getAccessToken());
  }

  /**
   * Checks if an application exists.
   *
   * @param app the application to check
   * @return true if the application exists
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #exists(ApplicationId)} instead.
   */
  @Deprecated
  public boolean exists(Id.Application app) throws IOException, UnauthenticatedException, UnauthorizedException {
    return exists(app.toEntityId());
  }

  /**
   * Checks if an application exists.
   *
   * @param app the application to check
   * @return true if the application exists
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public boolean exists(ApplicationId app) throws IOException, UnauthenticatedException, UnauthorizedException {
    HttpResponse response = restClient.execute(
      HttpMethod.GET,
      config.resolveNamespacedURLV3(app.getParent(), "apps/" + app.getApplication()),
      config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    return response.getResponseCode() != HttpURLConnection.HTTP_NOT_FOUND;
  }

  /**
   * Waits for an application to be deployed.
   *
   * @param app the application to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the application was not yet deployed before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   * @deprecated since 4.0.0. Use {@link #waitForDeployed(ApplicationId, long, TimeUnit)} instead.
   */
  @Deprecated
  public void waitForDeployed(final Id.Application app, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthenticatedException, TimeoutException, InterruptedException {
    waitForDeployed(app.toEntityId(), timeout, timeoutUnit);
  }

  /**
   * Waits for an application to be deployed.
   *
   * @param app the application to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the application was not yet deployed before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeployed(final ApplicationId app, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthenticatedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(app);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthenticatedException.class);
    }
  }

  /**
   * Waits for an application to be deleted.
   *
   * @param app the application to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the application was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   * @deprecated since 4.0.0. Use {@link #waitForDeleted(ApplicationId, long, TimeUnit)} instead.
   */
  @Deprecated
  public void waitForDeleted(final Id.Application app, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthenticatedException, TimeoutException, InterruptedException {
    waitForDeleted(app.toEntityId(), timeout, timeoutUnit);
  }

  /**
   * Waits for an application to be deleted.
   *
   * @param app the application to check
   * @param timeout time to wait before timing out
   * @param timeoutUnit time unit of timeout
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the application was not yet deleted before {@code timeout} milliseconds
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForDeleted(final ApplicationId app, long timeout, TimeUnit timeoutUnit)
    throws IOException, UnauthenticatedException, TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(false, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return exists(app);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class, UnauthenticatedException.class);
    }
  }

  /**
   * Deploys an application.
   *
   * @param jarFile JAR file of the application to deploy
   * @throws IOException if a network error occurred
   * @deprecated since 4.0.0. Use {@link #deploy(NamespaceId, File)} instead.
   */
  @Deprecated
  public void deploy(Id.Namespace namespace, File jarFile) throws IOException, UnauthenticatedException {
    deploy(namespace.toEntityId(), jarFile);
  }

  /**
   * Deploys an application.
   *
   * @param jarFile JAR file of the application to deploy
   * @throws IOException if a network error occurred
   */
  public void deploy(NamespaceId namespace, File jarFile) throws IOException, UnauthenticatedException {
    Map<String, String> headers = ImmutableMap.of("X-Archive-Name", jarFile.getName());
    deployApp(namespace, jarFile, headers);
  }

  /**
   * Deploys an application with a serialized application configuration string.
   *
   * @param namespace namespace to which the application should be deployed
   * @param jarFile JAR file of the application to deploy
   * @param appConfig serialized application configuration
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #deploy(NamespaceId, File, String)} instead.
   */
  @Deprecated
  public void deploy(Id.Namespace namespace, File jarFile,
                     String appConfig) throws IOException, UnauthenticatedException {
    deploy(namespace.toEntityId(), jarFile, appConfig);
  }

  /**
   * Deploys an application with a serialized application configuration string.
   *
   * @param namespace namespace to which the application should be deployed
   * @param jarFile JAR file of the application to deploy
   * @param appConfig serialized application configuration
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void deploy(NamespaceId namespace, File jarFile,
                     @Nullable String appConfig) throws IOException, UnauthenticatedException {
    deploy(namespace, jarFile, appConfig, null);
  }

  /**
   * Deploys an application with a serialized application configuration string and an owner.
   *
   * @param namespace namespace to which the application should be deployed
   * @param jarFile JAR file of the application to deploy
   * @param appConfig serialized application configuration
   * @param ownerPrincipal the principal of application owner
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void deploy(NamespaceId namespace, File jarFile,
                     @Nullable String appConfig,
                     @Nullable String ownerPrincipal) throws IOException, UnauthenticatedException {
    Map<String, String> headers = new HashMap<>();
    headers.put("X-Archive-Name", jarFile.getName());
    if (appConfig != null) {
      headers.put("X-App-Config", appConfig);
    }
    if (ownerPrincipal != null) {
      headers.put("X-Principal", ownerPrincipal);
    }
    deployApp(namespace, jarFile, headers);
  }

  /**
   * Deploys an application with an application configuration object.
   *
   * @param namespace namespace to which the application should be deployed
   * @param jarFile JAR file of the application to deploy
   * @param appConfig application configuration object
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #deploy(NamespaceId, File, Config)} instead.
   */
  @Deprecated
  public void deploy(Id.Namespace namespace, File jarFile,
                     Config appConfig) throws IOException, UnauthenticatedException {
    deploy(namespace.toEntityId(), jarFile, appConfig);
  }

  /**
   * Deploys an application with an application configuration object.
   *
   * @param namespace namespace to which the application should be deployed
   * @param jarFile JAR file of the application to deploy
   * @param appConfig application configuration object
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void deploy(NamespaceId namespace, File jarFile,
                     Config appConfig) throws IOException, UnauthenticatedException {
    deploy(namespace, jarFile, GSON.toJson(appConfig));
  }

  /**
   * Deploys an application using an existing artifact.
   *
   * @param appId the id of the application to add
   * @param createRequest the request body, which contains the artifact to use and any application config
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #deploy(ApplicationId, AppRequest)} instead
   */
  @Deprecated
  public void deploy(Id.Application appId, AppRequest<?> createRequest) throws IOException, UnauthenticatedException {
    deploy(appId.toEntityId(), createRequest);
  }

  /**
   * Creates an application with a version using an existing artifact.
   *
   * @param appId the id of the application to add
   * @param createRequest the request body, which contains the artifact to use and any application config
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void deploy(ApplicationId appId, AppRequest<?> createRequest) throws IOException, UnauthenticatedException {
    URL url = config.resolveNamespacedURLV3(new NamespaceId(appId.getNamespace()),
                                            String.format("apps/%s/versions/%s/create",
                                                          appId.getApplication(), appId.getVersion()));
    HttpRequest request = HttpRequest.post(url)
      .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
      .withBody(GSON.toJson(createRequest))
      .build();
    restClient.upload(request, config.getAccessToken());
  }

  /**
   * Update an existing app to use a different artifact version or config.
   *
   * @param appId the id of the application to update
   * @param updateRequest the request to update the application with
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or requested artifact could not be found
   * @throws BadRequestException if the request is invalid
   * @deprecated since 4.0.0. Use {@link #update(ApplicationId, AppRequest)} instead.
   */
  @Deprecated
  public void update(Id.Application appId, AppRequest<?> updateRequest)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    update(appId.toEntityId(), updateRequest);
  }


  /**
   * Update an existing app to use a different artifact version or config.
   *
   * @param appId the id of the application to update
   * @param updateRequest the request to update the application with
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or requested artifact could not be found
   * @throws BadRequestException if the request is invalid
   */
  public void update(ApplicationId appId, AppRequest<?> updateRequest)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(appId.getParent(), String.format("apps/%s/update", appId.getApplication()));
    HttpRequest request = HttpRequest.post(url)
      .withBody(GSON.toJson(updateRequest))
      .build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND, HttpURLConnection.HTTP_BAD_REQUEST);

    int responseCode = response.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("app or app artifact");
    } else if (responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(String.format("Bad Request. Reason: %s",
                                                  response.getResponseBodyAsString()));
    }
  }

  private void deployApp(NamespaceId namespace, File jarFile, Map<String, String> headers)
    throws IOException, UnauthenticatedException {
    URL url = config.resolveNamespacedURLV3(namespace, "apps");
    HttpRequest request = HttpRequest.post(url)
      .addHeaders(headers)
      .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM)
      .withBody(jarFile).build();
    restClient.upload(request, config.getAccessToken());
  }

  /**
   * Lists all programs of a type.
   *
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #listAllPrograms(NamespaceId, ProgramType)} instead.
   */
  @Deprecated
  public List<ProgramRecord> listAllPrograms(Id.Namespace namespace, ProgramType programType)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return listAllPrograms(namespace.toEntityId(), programType);
  }

  /**
   * Lists all programs of a type.
   *
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<ProgramRecord> listAllPrograms(NamespaceId namespace, ProgramType programType)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    Preconditions.checkArgument(programType.isListable());

    String path = programType.getCategoryName();
    URL url = config.resolveNamespacedURLV3(namespace, path);
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
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #listAllPrograms(NamespaceId)} instead.
   */
  @Deprecated
  public Map<ProgramType, List<ProgramRecord>> listAllPrograms(Id.Namespace namespace)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return listAllPrograms(namespace.toEntityId());
  }

  /**
   * Lists all programs.
   *
   * @return list of {@link ProgramRecord}s
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public Map<ProgramType, List<ProgramRecord>> listAllPrograms(NamespaceId namespace)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    ImmutableMap.Builder<ProgramType, List<ProgramRecord>> allPrograms = ImmutableMap.builder();
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isListable()) {
        List<ProgramRecord> programRecords = Lists.newArrayList();
        programRecords.addAll(listAllPrograms(namespace, programType));
        allPrograms.put(programType, programRecords);
      }
    }
    return allPrograms.build();
  }

  /**
   * Lists programs of a type belonging to an application.
   *
   * @param app the application
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #listPrograms(ApplicationId, ProgramType)} instead.
   */
  @Deprecated
  public List<ProgramRecord> listPrograms(Id.Application app, ProgramType programType)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return listPrograms(app.toEntityId(), programType);
  }

  /**
   * Lists programs of a type belonging to an application.
   *
   * @param app the application
   * @param programType type of the programs to list
   * @return list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<ProgramRecord> listPrograms(ApplicationId app, ProgramType programType)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    Preconditions.checkArgument(programType.isListable());

    List<ProgramRecord> programs = Lists.newArrayList();
    for (ProgramRecord program : listPrograms(app)) {
      if (programType.equals(program.getType())) {
        programs.add(program);
      }
    }
    return programs;
  }

  /**
   * Lists programs of a type belonging to an application.
   *
   * @param app the application
   * @return Map of {@link ProgramType} to list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Use {@link #listProgramsByType(ApplicationId)} instead.
   */
  @Deprecated
  public Map<ProgramType, List<ProgramRecord>> listProgramsByType(Id.Application app)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return listProgramsByType(app.toEntityId());
  }

  /**
   * Lists programs of a type belonging to an application.
   *
   * @param app the application
   * @return Map of {@link ProgramType} to list of {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public Map<ProgramType, List<ProgramRecord>> listProgramsByType(ApplicationId app)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    Map<ProgramType, List<ProgramRecord>> result = Maps.newHashMap();
    for (ProgramType type : ProgramType.values()) {
      result.put(type, Lists.<ProgramRecord>newArrayList());
    }
    for (ProgramRecord program : listPrograms(app)) {
      result.get(program.getType()).add(program);
    }
    return result;
  }

  /**
   * Lists all programs belonging to an application.
   *
   * @param app the application
   * @return List of all {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #listPrograms(ApplicationId)} instead
   */
  @Deprecated
  public List<ProgramRecord> listPrograms(Id.Application app)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return listPrograms(app.toEntityId());
  }

  /**
   * Lists all programs belonging to an application.
   *
   * @param app the application
   * @return List of all {@link ProgramRecord}s
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<ProgramRecord> listPrograms(ApplicationId app)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("apps/%s/versions/%s", app.getApplication(), app.getVersion());
    URL url = config.resolveNamespacedURLV3(app.getParent(), path);
    HttpRequest request = HttpRequest.get(url).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(app);
    }

    return ObjectResponse.fromJsonBody(response, ApplicationDetail.class).getResponseObject().getPrograms();
  }

  /**
   * Add a schedule to an application.
   *
   * @param app the application
   * @param scheduleDetail the schedule to be added
   */
  public void addSchedule(ApplicationId app, ScheduleDetail scheduleDetail)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException,
    UnauthorizedException, BadRequestException {
    String path = String.format("apps/%s/versions/%s/schedules/%s", app.getApplication(), app.getVersion(),
                                scheduleDetail.getName());
    HttpResponse response = restClient.execute(HttpMethod.PUT, config.resolveNamespacedURLV3(app.getParent(), path),
                                               GSON.toJson(scheduleDetail), ImmutableMap.<String, String>of(),
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);

    int responseCode = response.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(app);
    } else if (responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(String.format("Bad Request. Reason: %s",
                                                  response.getResponseBodyAsString()));
    }
  }

  /**
   * Enable a schedule in an application.
   *
   * @param scheduleId the id of the schedule to be enabled
   */
  public void enableSchedule(ScheduleId scheduleId)
    throws ApplicationNotFoundException, IOException, UnauthenticatedException,
    UnauthorizedException, BadRequestException {
    String path = String.format("apps/%s/versions/%s/program-type/schedules/program-id/%s/action/enable",
                                scheduleId.getApplication(), scheduleId.getVersion(),
                                scheduleId.getSchedule());
    HttpResponse response = restClient.execute(HttpMethod.PUT,
                                               config.resolveNamespacedURLV3(scheduleId.getParent().getParent(), path),
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);

    int responseCode = response.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(scheduleId.getParent());
    } else if (responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(String.format("Bad Request. Reason: %s",
                                                  response.getResponseBodyAsString()));
    }
  }
}
