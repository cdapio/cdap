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
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
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
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Returns the Preferences stored at the Instance Level.
   *
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public Map<String, String> getInstancePreferences() throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("preferences");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Instance Level.
   *
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void setInstancePreferences(Map<String, String> preferences) throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("preferences");
    restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null, config.getAccessToken());
  }

  /**
   * Deletes Preferences at the Instance Level.
   *
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void deleteInstancePreferences() throws IOException, UnauthorizedException {
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public Map<String, String> getNamespacePreferences(Id.Namespace namespace, boolean resolved)
    throws IOException, UnauthorizedException, NotFoundException {

    String res = Boolean.toString(resolved);
    URL url = config.resolveURLV3(String.format("namespaces/%s/preferences?resolved=%s",
                                                namespace.getId(), res));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void setNamespacePreferences(Id.Namespace namespace, Map<String, String> preferences) throws IOException,
    UnauthorizedException, NotFoundException {
    URL url = config.resolveURLV3(String.format("namespaces/%s/preferences", namespace));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void deleteNamespacePreferences(Id.Namespace namespace)
    throws IOException, UnauthorizedException, NotFoundException {

    URL url = config.resolveURLV3(String.format("namespaces/%s/preferences", namespace.getId()));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ApplicationNotFoundException if the requested application is not found
   */
  public Map<String, String> getApplicationPreferences(Id.Application application, boolean resolved)
    throws IOException, UnauthorizedException, NotFoundException {

    String res = Boolean.toString(resolved);
    URL url = config.resolveURLV3(String.format("namespaces/%s/apps/%s/preferences?resolved=%s",
                                                application.getNamespaceId(), application.getId(), res));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested application or namespace is not found
   */
  public void setApplicationPreferences(Id.Application application, Map<String, String> preferences)
    throws IOException, UnauthorizedException, NotFoundException {

    URL url = config.resolveURLV3(String.format("namespaces/%s/apps/%s/preferences",
                                                application.getNamespaceId(), application.getId()));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the request application or namespace is not found
   */
  public void deleteApplicationPreferences(Id.Application application)
    throws IOException, UnauthorizedException, NotFoundException {

    URL url = config.resolveURLV3(String.format("namespaces/%s/apps/%s/preferences",
                                                application.getNamespaceId(), application.getId()));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public Map<String, String> getProgramPreferences(Id.Program program, boolean resolved)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    String res = Boolean.toString(resolved);
    URL url = config.resolveURLV3(String.format("namespaces/%s/apps/%s/%s/%s/preferences?resolved=%s",
                                                program.getNamespaceId(), program.getApplicationId(),
                                                program.getType().getCategoryName(), program.getId(), res));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void setProgramPreferences(Id.Program program, Map<String, String> preferences)
    throws IOException, UnauthorizedException, ProgramNotFoundException {
    URL url = config.resolveURLV3(String.format("/namespaces/%s/apps/%s/%s/%s/preferences",
                                                program.getNamespaceId(), program.getApplicationId(),
                                                program.getType().getCategoryName(), program.getId()));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void deleteProgramPreferences(Id.Program program)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    URL url = config.resolveURLV3(String.format("namespaces/%s/apps/%s/%s/%s/preferences",
                                                program.getNamespaceId(), program.getApplicationId(),
                                                program.getType().getCategoryName(), program.getId()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }
}
