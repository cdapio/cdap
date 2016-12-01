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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP User Services.
 */
@Beta
public class ServiceClient {

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_INTEGER_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();
  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ServiceClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public ServiceClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Gets a {@link ServiceSpecification} for a {@link Service}.
   *
   * @param service ID of the service
   * @return {@link ServiceSpecification} representing the service
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   * @deprecated since 4.0.0. Please use {@link #get(ProgramId)} instead
   */
  @Deprecated
  public ServiceSpecification get(Id.Service service)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    return get(service.toEntityId());
  }

  /**
   * Gets a {@link ServiceSpecification} for a {@link Service}.
   *
   * @param service ID of the service
   * @return {@link ServiceSpecification} representing the service
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   */
  public ServiceSpecification get(ProgramId service)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(service.getNamespaceId(),
                                            String.format("apps/%s/versions/%s/services/%s", service.getApplication(),
                                                          service.getVersion(), service.getProgram()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service);
    }
    return ObjectResponse.fromJsonBody(response, ServiceSpecification.class).getResponseObject();
  }

  /**
   * Gets a list of {@link ServiceHttpEndpoint} that a {@link Service} exposes.
   *
   * @param service ID of the service
   * @return A list of {@link ServiceHttpEndpoint}
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   * @deprecated since 4.0.0. Please use {@link #getEndpoints(ServiceId)} instead
   */
  @Deprecated
  public List<ServiceHttpEndpoint> getEndpoints(Id.Service service)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    return getEndpoints(service.toEntityId());
  }

  /**
   * Gets a list of {@link ServiceHttpEndpoint} that a {@link Service} exposes.
   *
   * @param service ID of the service
   * @return A list of {@link ServiceHttpEndpoint}
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   */
  public List<ServiceHttpEndpoint> getEndpoints(ServiceId service)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    ServiceSpecification specification = get(service);
    ImmutableList.Builder<ServiceHttpEndpoint> builder = new ImmutableList.Builder<>();
    for (HttpServiceHandlerSpecification handlerSpecification : specification.getHandlers().values()) {
      builder.addAll(handlerSpecification.getEndpoints());
    }
    return builder.build();
  }

  /**
   * Checks whether the {@link Service} is active. Returns without throwing any exception if it is active.
   *
   * @param service ID of the service
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   * @throws ServiceUnavailableException if the service is not available
   * @deprecated since 4.0.0. Please use {@link #checkAvailability(ServiceId)} instead
   */
  @Deprecated
  public void checkAvailability(Id.Service service) throws IOException, UnauthenticatedException, NotFoundException,
    ServiceUnavailableException, UnauthorizedException {
    checkAvailability(service.toEntityId());
  }

  /**
   * Checks whether the {@link Service} is active.
   *
   * @param service ID of the service
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   * @throws ServiceUnavailableException if the service is not available
   */
  public void checkAvailability(ServiceId service) throws IOException, UnauthenticatedException, NotFoundException,
    ServiceUnavailableException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(service.getNamespaceId(),
                                            String.format("apps/%s/versions/%s/services/%s/available",
                                                          service.getApplication(), service.getVersion(),
                                                          service.getProgram()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND, HttpURLConnection.HTTP_BAD_REQUEST,
                                               HttpURLConnection.HTTP_UNAVAILABLE);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service);
    }

    if (response.getResponseCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
      throw new ServiceUnavailableException(service.getProgram());
    }
  }

  /**
   * Gets a {@link URL} to call methods for a {@link Service}. If multiple versions exist, traffic to the {@link URL}
   * will be distributed to each version according to routing strategy. If only one version exists, this version
   * will always be reached with the {@link URL}.
   *
   * @param service ID of the service
   * @return a URL to call methods of the service
   * @throws NotFoundException @throws NotFoundException if the app or service could not be found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #getServiceURL(ServiceId)} or
   *             {@link #getVersionedServiceURL(ServiceId)} instead
   */
  public URL getServiceURL(Id.Service service)
    throws NotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return getServiceURL(service.toEntityId());
  }

  /**
   * Gets a {@link URL} to call methods for a specific version of a {@link Service}.
   *
   * @param service {@link ServiceId} of the service
   * @return a URL to call methods of the service
   * @throws NotFoundException @throws NotFoundException if the app or service could not be found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public URL getVersionedServiceURL(ServiceId service)
    throws NotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
      // Make sure the service actually exists
      get(service);
      return config.resolveNamespacedURLV3(service.getNamespaceId(),
                                           String.format("apps/%s/versions/%s/services/%s/methods/",
                                                         service.getApplication(), service.getVersion(),
                                                         service.getEntityName()));
  }

  /**
   * Gets a {@link URL} to call methods for the endpoint of a {@link Service} with no application version. If multiple
   * versions exist, traffic to the {@link URL} will be distributed to each version according to routing strategy. If
   * only one version exists, this version will always be reached with the {@link URL}.
   *
   * @param service {@link ServiceId} of the service
   * @return a URL to call methods of the service
   * @throws NotFoundException @throws NotFoundException if the app or service could not be found
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public URL getServiceURL(ServiceId service)
    throws NotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return config.resolveNamespacedURLV3(service.getNamespaceId(),
                                         String.format("apps/%s/services/%s/methods/",
                                                       service.getApplication(), service.getEntityName()));
  }

  /**
   * Gets RouteConfig of a service with different application versions.
   *
   * @param serviceId {@link ServiceId} of the service with the application version part ignored
   * @return a Map of {@link String} application version
   *         and non-zero {@link Integer} percentage of traffic routed to the version.
   */
  public Map<String, Integer> getRouteConfig(ServiceId serviceId)
    throws UnauthorizedException, IOException, UnauthenticatedException {
    URL url = buildRouteConfigUrl(serviceId);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.<Map<String, Integer>>fromJsonBody(response, MAP_STRING_INTEGER_TYPE).getResponseObject();
  }

  /**
   * Stores RouteConfig of a service with different application versions.
   *
   * @param serviceId {@link ServiceId} of the service with the application version part ignored
   * @param routeConfig a Map of {@link String} application version and {@link Integer} percentage of
   *                    traffic routed to the version.
   */
  public void storeRouteConfig(ServiceId serviceId, Map<String, Integer> routeConfig)
    throws IOException, UnauthorizedException, UnauthenticatedException {
    URL url = buildRouteConfigUrl(serviceId);
    HttpRequest request = HttpRequest.put(url)
      .withBody(GSON.toJson(routeConfig, MAP_STRING_INTEGER_TYPE)).build();
    restClient.upload(request, config.getAccessToken());
  }

  /**
   * Deletes RouteConfig of a service with different application versions.
   *
   * @param serviceId {@link ServiceId} of the service with the application version part ignored
   */
  public void deleteRouteConfig(ServiceId serviceId)
    throws IOException, UnauthorizedException, UnauthenticatedException {
    URL url = buildRouteConfigUrl(serviceId);
    restClient.execute(HttpMethod.DELETE, url, config.getAccessToken());
  }

  /**
   * Calls the non-versioned service endpoint for a given method and get routed to a specific version by the Router
   *
   * @param serviceId {@link ServiceId} of the service with the application version part ignored
   * @param methodPath the path specifying only the method
   * @return {@link HttpResponse} from the service method
   */
  public HttpResponse callServiceMethod(ServiceId serviceId, String methodPath)
    throws IOException, UnauthorizedException, UnauthenticatedException {
    String path = String.format("apps/%s/services/%s/methods/%s", serviceId.getApplication(), serviceId.getEntityName(),
                                methodPath);
    URL url = config.resolveNamespacedURLV3(serviceId.getNamespaceId(), path);
    return restClient.execute(HttpMethod.GET, url, config.getAccessToken());
  }

  /**
   * Constructs URL to reach RouteConfig REST API endpoints of a service.
   *
   * @param serviceId {@link ServiceId} of the service with the application version part ignored
   */
  private URL buildRouteConfigUrl(ServiceId serviceId)
    throws MalformedURLException {
    String path = String.format("apps/%s/services/%s/routeconfig", serviceId.getApplication(),
                                serviceId.getEntityName());
    return config.resolveNamespacedURLV3(serviceId.getNamespaceId(), path);
  }
}
