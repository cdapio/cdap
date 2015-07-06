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
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceNotEnabledException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.SystemServiceLiveInfo;
import co.cask.cdap.proto.SystemServiceMeta;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to monitor CDAP.
 */
public class MonitorClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public MonitorClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public MonitorClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Gets the live info of a system service.
   *
   * @param serviceName Name of the system service
   * @return live info of the system service
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public SystemServiceLiveInfo getSystemServiceLiveInfo(String serviceName)
    throws IOException, UnauthorizedException, NotFoundException {

    Id.SystemService systemService = Id.SystemService.from(serviceName);
    URL url = config.resolveURLV3(String.format("system/services/%s/live-info", serviceName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    String responseBody = new String(response.getResponseBody());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(systemService);
    }
    return GSON.fromJson(responseBody, SystemServiceLiveInfo.class);
  }

  /**
   * Lists all system services.
   *
   * @return list of {@link SystemServiceMeta}s.
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<SystemServiceMeta> listSystemServices() throws IOException, UnauthorizedException {
    URL url = config.resolveURL("system/services");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<SystemServiceMeta>>() { }).getResponseObject();
  }

  /**
   * Gets the status of a system service.
   *
   * @param serviceName Name of the system service
   * @return status of the system service
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the system service with the specified name could not be found
   * @throws BadRequestException if the operation was not valid for the system service
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public String getSystemServiceStatus(String serviceName)
    throws IOException, NotFoundException, BadRequestException, UnauthorizedException {

    Id.SystemService systemService = Id.SystemService.from(serviceName);
    URL url = config.resolveURL(String.format("system/services/%s/status", serviceName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    String responseBody = new String(response.getResponseBody());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(systemService);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(responseBody);
    }
    Map<String, String> status = GSON.fromJson(responseBody, new TypeToken<Map<String, String>>() { }.getType());
    return status.get("status");
  }

  /**
   * Gets the status of all system services.
   *
   * @return map from service name to service status
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public Map<String, String> getAllSystemServiceStatus() throws IOException, UnauthorizedException {
    URL url = config.resolveURL("system/services/status");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * @return true if all system services' status is 'OK'; false otherwise
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public boolean allSystemServicesOk() throws IOException, UnauthorizedException {
    for (String status : getAllSystemServiceStatus().values()) {
      if (!"OK".equals(status)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Sets the number of instances the system service is running on.
   *
   * @param serviceName name of the system service
   * @param instances number of instances the system service is running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the system service with the specified name was not found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void setSystemServiceInstances(String serviceName, int instances)
    throws IOException, NotFoundException, BadRequestException, UnauthorizedException {

    Id.SystemService systemService = Id.SystemService.from(serviceName);
    URL url = config.resolveURL(String.format("system/services/%s/instances", serviceName));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(systemService);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(new String(response.getResponseBody()));
    }
  }

  /**
   * Gets the number of instances the system service is running on.
   *
   * @param serviceName name of the system service
   * @return number of instances the system service is running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the system service with the specified name was not found
   * @throws ServiceNotEnabledException if the system service is not currently enabled
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public int getSystemServiceInstances(String serviceName)
    throws IOException, NotFoundException, ServiceNotEnabledException, UnauthorizedException {

    Id.SystemService systemService = Id.SystemService.from(serviceName);
    URL url = config.resolveURL(String.format("system/services/%s/instances", serviceName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_FORBIDDEN);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(systemService);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
      throw new ServiceNotEnabledException(serviceName);
    }

    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

}
