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
import co.cask.cdap.client.exception.NotFoundException;
import co.cask.cdap.client.exception.ProgramNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramType;
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
  public PreferencesClient(ClientConfig config) {
    this.config = new ClientConfig.Builder(config).setApiVersion(Constants.Gateway.API_VERSION_3_TOKEN).build();
    this.restClient = RESTClient.create(config);
  }

  /**
   * Returns the Preferences stored at the Instance Level.
   *
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public Map<String, String> getInstancePreferences() throws IOException, UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("configuration/preferences");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Instance Level.
   *
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void setInstancePreferences(Map<String, String> preferences) throws IOException,
    UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("configuration/preferences");
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken());
  }

  /**
   * Deletes Preferences at the Instance Level.
   *
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void deleteInstancePreferences() throws IOException, UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("configuration/preferences");
    restClient.execute(HttpMethod.DELETE, url, config.getAccessToken());
  }

  /**
   * Returns the Preferences stored at the Namespace Level.
   *
   * @param namespace Namespace Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public Map<String, String> getNamespacePreferences(String namespace, boolean resolved) throws IOException,
    UnAuthorizedAccessTokenException, NotFoundException {
    String res = Boolean.toString(resolved);
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s?resolved=%s", namespace, res));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("namespace", namespace);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Namespace Level.
   *
   * @param namespace Namespace Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void setNamespacePreferences(String namespace, Map<String, String> preferences) throws IOException,
    UnAuthorizedAccessTokenException, NotFoundException {
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s", namespace));
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("namespace", namespace);
    }
  }

  /**
   * Deletes Preferences at the Namespace Level.
   *
   * @param namespace Namespace Id
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void deleteNamespacePreferences(String namespace) throws IOException, UnAuthorizedAccessTokenException,
    NotFoundException {
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s", namespace));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("namespace", namespace);
    }
  }

  /**
   * Returns the Preferences stored at the Application Level.
   *
   * @param namespace Namespace Id
   * @param application Application Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested application or namespace is not found
   */
  public Map<String, String> getApplicationPreferences(String namespace, String application, boolean resolved)
    throws IOException, UnAuthorizedAccessTokenException, NotFoundException {
    String res = Boolean.toString(resolved);
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s/apps/%s?resolved=%s",
                                              namespace, application, res));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("namespace or application", namespace + "/" + application);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Application Level.
   *
   * @param namespace Namespace Id
   * @param application Application Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested application or namespace is not found
   */
  public void setApplicationPreferences(String namespace, String application, Map<String, String> preferences)
    throws IOException, UnAuthorizedAccessTokenException, NotFoundException {
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s/apps/%s",
                                              namespace, application));
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("namespace or application", namespace + "/" + application);
    }
  }

  /**
   * Deletes Preferences at the Application Level.
   * 
   * @param namespace Namespace Id
   * @param application Application Id
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the request application or namespace is not found
   */
  public void deleteApplicationPreferences(String namespace, String application)
    throws IOException, UnAuthorizedAccessTokenException, NotFoundException {
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s/apps/%s",
                                              namespace, application));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("namespace or application", namespace + "/" + application);
    }
  }

  /**
   * Returns the Preferences stored at the Program Level.
   *
   * @param namespace Namespace Id
   * @param application Application Id
   * @param programType Program Type
   * @param programId Program Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public Map<String, String> getProgramPreferences(String namespace, String application, String programType,
                                                   String programId, boolean resolved)
    throws IOException, UnAuthorizedAccessTokenException, ProgramNotFoundException {
    String res = Boolean.toString(resolved);
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s/apps/%s/%s/%s?resolved=%s",
                                              namespace, application, programType, programId, res));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(ProgramType.valueOfCategoryName(programType), application, programId);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Program Level.
   *
   * @param namespace Namespace Id
   * @param application Application Id
   * @param programType Program Type
   * @param programId Program Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void setProgramPreferences(String namespace, String application, String programType, String programId,
                                    Map<String, String> preferences)
    throws IOException, UnAuthorizedAccessTokenException, ProgramNotFoundException {
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s/apps/%s/%s/%s",
                                              namespace, application, programType, programId));
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(ProgramType.valueOfCategoryName(programType), application, programId);
    }
  }

  /**
   * Deletes Preferences at the Program Level.
   *
   * @param namespace Namespace Id
   * @param application Application Id
   * @param programType Program Type
   * @param programId Program Id
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void deleteProgramPreferences(String namespace, String application, String programType, String programId)
    throws IOException, UnAuthorizedAccessTokenException, ProgramNotFoundException {
    URL url = config.resolveURL(String.format("configuration/preferences/namespaces/%s/apps/%s/%s/%s",
                                              namespace, application, programType, programId));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(ProgramType.valueOfCategoryName(programType), application, programId);
    }
  }
}
