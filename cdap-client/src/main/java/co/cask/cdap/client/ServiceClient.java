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

import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP User Services.
 */
public class ServiceClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ServiceClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public ServiceClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Gets a {@link ServiceSpecification} for a {@link Service}.
   *
   * @param appId ID of the application that the service belongs to
   * @param serviceId ID of the service
   * @return {@link ServiceSpecification} representing the service
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   */
  public ServiceSpecification get(String appId, String serviceId)
    throws IOException, UnauthorizedException, NotFoundException {

    Id.Application app = Id.Application.from(config.getNamespace(), appId);
    Id.Service service = Id.Service.from(app, serviceId);

    URL url = config.resolveNamespacedURLV3(String.format("apps/%s/services/%s", appId, serviceId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service);
    }
    return ObjectResponse.fromJsonBody(response, ServiceSpecification.class).getResponseObject();
  }

  /**
   * Gets a list of {@link ServiceHttpEndpoint} that a {@link Service} exposes
   * @param appId ID of the application that the service belongs to
   * @param serviceId ID of the service
   * @return A list of {@link ServiceHttpEndpoint}
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the app or service could not be found
   */
  public List<ServiceHttpEndpoint> getEndpoints(String appId, String serviceId)
    throws IOException, UnauthorizedException, NotFoundException {

    ServiceSpecification specification = get(appId, serviceId);
    ImmutableList.Builder<ServiceHttpEndpoint> builder = new ImmutableList.Builder<>();
    for (HttpServiceHandlerSpecification handlerSpecification : specification.getHandlers().values()) {
      builder.addAll(handlerSpecification.getEndpoints());
    }
    return builder.build();
  }

  public URL getServiceURL(String appId, String serviceId)
    throws NotFoundException, IOException, UnauthorizedException {
    // Make sure the service actually exists
    get(appId, serviceId);
    return config.resolveNamespacedURLV3(String.format("apps/%s/services/%s/methods/", appId, serviceId));
  }
}
