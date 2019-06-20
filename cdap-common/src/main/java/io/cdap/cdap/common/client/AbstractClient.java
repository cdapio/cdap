/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.client;

import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * Abstract client to interact with app fabric service over HTTP.
 */
public abstract class AbstractClient {

  private final RemoteClient remoteClient;

  public AbstractClient(DiscoveryServiceClient discoveryClient) {
    HttpRequestConfig httpRequestConfig = new DefaultHttpRequestConfig(false);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP, httpRequestConfig,
                                         Constants.Gateway.API_VERSION_3);
  }

  /**
   * Executes an HTTP request.
   */
  protected HttpResponse execute(HttpRequest request, int... allowedErrorCodes)
    throws IOException, UnauthorizedException {
    return remoteClient.execute(request);
  }

  /**
   * Resolved the specified URL
   */
  protected URL resolve(String resource) {
    return remoteClient.resolve(resource);
  }

  /**
   * Makes the HTTP request
   *
   * @param path the endpoint path
   * @param httpMethod the http method
   * @param body the body of the request
   * @return the http response from the request
   * @throws IOException if there was an IOException while performing the request
   * @throws BadRequestException if the request is invalid
   * @throws UnauthorizedException if the current user is not authenticated
   */
  protected HttpResponse makeRequest(String path, HttpMethod httpMethod, @Nullable String body)
    throws IOException, BadRequestException, UnauthorizedException {
    URL url = resolve(path);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
    if (body != null) {
      builder.withBody(body);
    }
    HttpResponse response = execute(builder.build(),
                                    HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    return response;
  }

}
