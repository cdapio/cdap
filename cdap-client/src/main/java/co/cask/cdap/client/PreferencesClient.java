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
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Provides ways to get/set Preferences.
 */
@Beta
public class PreferencesClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public PreferencesClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public PreferencesClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Returns the Preferences stored at the Instance Level.
   *
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public Map<String, String> getInstancePreferences()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveURLV3("preferences");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Instance Level.
   *
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void setInstancePreferences(Map<String, String> preferences)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveURLV3("preferences");
    restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null, config.getAccessToken());
  }

  /**
   * Deletes Preferences at the Instance Level.
   *
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void deleteInstancePreferences() throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveURLV3("preferences");
    restClient.execute(HttpMethod.DELETE, url, config.getAccessToken());
  }

  /**
   * Returns the Preferences stored at the Namespace Level.
   *
   * @param namespace Namespace Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public Map<String, String> getNamespacePreferences(NamespaceId namespace, boolean resolved)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    String res = Boolean.toString(resolved);
    URL url = config.resolveURLV3(String.format("namespaces/%s/preferences?resolved=%s",
                                                namespace.getNamespace(), res));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespace);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Namespace Level.
   *
   * @param namespace Namespace Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void setNamespacePreferences(NamespaceId namespace, Map<String, String> preferences) throws IOException,
    UnauthenticatedException, NotFoundException, UnauthorizedException {
    URL url = config.resolveURLV3(String.format("namespaces/%s/preferences", namespace.getNamespace()));
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespace);
    }
  }

  /**
   * Deletes Preferences at the Namespace Level.
   *
   * @param namespace Namespace Id
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void deleteNamespacePreferences(NamespaceId namespace)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    URL url = config.resolveURLV3(String.format("namespaces/%s/preferences", namespace.getNamespace()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespace);
    }
  }

  /**
   * Returns the Preferences stored at the Application Level.
   *
   * @param application Application Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws ApplicationNotFoundException if the requested application is not found
   */
  public Map<String, String> getApplicationPreferences(ApplicationId application, boolean resolved)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    String res = Boolean.toString(resolved);
    URL url = config.resolveNamespacedURLV3(application.getParent(),
                                            String.format("/apps/%s/preferences?resolved=%s",
                                                          application.getApplication(), res));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(application);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Application Level.
   *
   * @param application Application Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested application or namespace is not found
   */
  public void setApplicationPreferences(ApplicationId application, Map<String, String> preferences)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(application.getParent(),
                                            String.format("/apps/%s/preferences", application.getApplication()));
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(application);
    }
  }

  /**
   * Deletes Preferences at the Application Level.
   *
   * @param application Application Id
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the request application or namespace is not found
   */
  public void deleteApplicationPreferences(ApplicationId application)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(application.getParent(),
                                            String.format("/apps/%s/preferences", application.getApplication()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(application);
    }
  }

  /**
   * Returns the Preferences stored at the Program Level.
   *
   * @param program Program Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public Map<String, String> getProgramPreferences(ProgramId program, boolean resolved)
    throws IOException, UnauthenticatedException, ProgramNotFoundException, UnauthorizedException {

    String res = Boolean.toString(resolved);
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(),
                                            String.format("/apps/%s/%s/%s/preferences?resolved=%s",
                                                          program.getApplication(),
                                                          program.getType().getCategoryName(),
                                                          program.getProgram(), res));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Program Level.
   *
   * @param program Program Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void setProgramPreferences(ProgramId program, Map<String, String> preferences)
    throws IOException, UnauthenticatedException, ProgramNotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(),
                                            String.format("/apps/%s/%s/%s/preferences",
                                                          program.getApplication(),
                                                          program.getType().getCategoryName(), program.getProgram()));
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }

  /**
   * Deletes Preferences at the Program Level.
   *
   * @param program Program Id
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void deleteProgramPreferences(ProgramId program)
    throws IOException, UnauthenticatedException, ProgramNotFoundException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(),
                                            String.format("/apps/%s/%s/%s/preferences",
                                                          program.getApplication(),
                                                          program.getType().getCategoryName(), program.getProgram()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }
}
