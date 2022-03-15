/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Fetch health check via internal REST API calls
 */
public class RemoteHealthCheckFetcher implements HealthCheckFetcher {
  private static final Gson GSON = new Gson();

  private final RemoteClientFactory remoteClientFactory;
  private RemoteClient remoteClient;

  @Inject
  public RemoteHealthCheckFetcher(RemoteClientFactory remoteClientFactory) {
    this.remoteClientFactory = remoteClientFactory;
  }

  /**
   * Get the application detail for the given application id
   */
  public Map<String, Object> getHealthDetails(String serviceName, String instanceName)
    throws IOException, NotFoundException, UnauthorizedException {
    /* Need to get the substring inside the serviceName:
    EX: serviceName = "cdap-bundle-test-v7-health-check-appfabric-service",
        instanceName = "bundle-test-v7",   instanceNameLength = length("bundle-test-v7-")
        updatedServiceName = "health.check.appfabric.service"
        The same as Constants.AppFabricHealthCheck.APP_FABRIC_HEALTH_CHECK_SERVICE
    */
    int instanceNameLength = instanceName.length() + 1;
    String updatedServiceName =
      serviceName.substring(serviceName.lastIndexOf(instanceName) + instanceNameLength).replace("-", ".");

    remoteClient = remoteClientFactory.createRemoteClient(updatedServiceName, new DefaultHttpRequestConfig(false),
                                                          Constants.Gateway.API_VERSION_3);

    String url = String.format("/health");
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse;
    httpResponse = execute(requestBuilder.build());
    Optional<Map<String, Object>> healthData = GSON.fromJson(httpResponse.getResponseBodyAsString(), Optional.class);
    return healthData.orElseGet(HashMap::new);
  }

  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException, UnauthorizedException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }
}
