/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.handlers;

import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.SystemServiceMeta;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Fetch Program logs via internal REST API calls
 */
public class RemoteMonitorServicesFetcher {
  private final RemoteClient remoteClient;

  @Inject
  public RemoteMonitorServicesFetcher(RemoteClientFactory remoteClientFactory) {
    this.remoteClient =
      remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP, new DefaultHttpRequestConfig(false),
                                             Gateway.API_VERSION_3);
  }

  /**
   * Lists all system services.
   *
   * @return list of {@link SystemServiceMeta}s.
   * @throws IOException              if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public Iterable<SystemServiceMeta> listSystemServices() throws NotFoundException, IOException {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(HttpMethod.GET, Constants.Monitor.SYSTEM_LOG_SERVICE_URL);
    HttpResponse response;
    response = execute(requestBuilder.build());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<SystemServiceMeta>>() {
    }).getResponseObject();
  }

  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException, UnauthorizedException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(String.format("Request failed %s with code %d ", httpResponse.getResponseBodyAsString(),
                                          httpResponse.getResponseCode()));
    }
    return httpResponse;
  }
}
