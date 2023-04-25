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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.api.app.AppStateStore;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/**
 * Remote implementation for application state read/write.
 */
public class RemoteAppStateStore implements AppStateStore {

  private static final String REMOTE_END_POINT = "/namespaces/%s/apps/%s/states/%s";

  private final RetryStrategy retryStrategy;
  private final RemoteClient remoteClient;
  private final String appName;
  private final String namespace;

  RemoteAppStateStore(CConfiguration cConf, RemoteClientFactory remoteClientFactory,
      String namespace,
      String appName) {
    this.namespace = namespace;
    this.appName = appName;
    this.remoteClient = remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP,
        new DefaultHttpRequestConfig(false),
        Constants.Gateway.INTERNAL_API_VERSION_3);
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf,
        Constants.AppFabric.APP_STATE + ".");
  }

  @Override
  public Optional<byte[]> getState(String key) throws IOException {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException(
          "Received null or empty value for required argument 'key'");
    }
    try {
      //Encode the key
      String encodedKey = Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
      return Retries.callWithRetries(() -> {
        HttpRequest.Builder requestBuilder =
            remoteClient.requestBuilder(HttpMethod.GET,
                String.format(REMOTE_END_POINT, namespace, appName, encodedKey));
        HttpResponse httpResponse = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
        int responseCode = httpResponse.getResponseCode();
        if (responseCode == 200) {
          byte[] responseBody = httpResponse.getResponseBody();
          if (responseBody.length == 0) {
            return Optional.empty();
          }
          return Optional.of(responseBody);
        }
        String notFoundExceptionMessage = String.format("Namespace %s or application %s not found.",
            namespace, appName, key);
        handleErrorResponse(responseCode, httpResponse.getResponseBodyAsString(),
            notFoundExceptionMessage);
        return null;
      }, retryStrategy);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void saveState(String key, byte[] value) throws IOException {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException(
          "Received null or empty value for required argument 'key'");
    }
    if (value == null || value.length == 0) {
      throw new IllegalArgumentException(
          "Received null or empty value for required argument 'value'");
    }
    try {
      String encodedKey = Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
      //Retries are ok since this is a PUT request
      Retries.runWithRetries(() -> {
        HttpRequest.Builder requestBuilder =
            remoteClient.requestBuilder(HttpMethod.PUT,
                    String.format(REMOTE_END_POINT, namespace, appName, encodedKey))
                .withBody(ByteBuffer.wrap(value));
        HttpResponse httpResponse = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
        int responseCode = httpResponse.getResponseCode();
        if (responseCode != 200) {
          String notFoundExceptionMessage = String.format(
              "Namespace %s or application %s not found.", namespace, appName);
          handleErrorResponse(responseCode, httpResponse.getResponseBodyAsString(),
              notFoundExceptionMessage);
        }
      }, retryStrategy);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteState(String key) throws IOException {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException(
          "Received null or empty value for required argument 'key'");
    }
    try {
      String encodedKey = Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
      Retries.runWithRetries(() -> {
        HttpRequest.Builder requestBuilder =
            remoteClient.requestBuilder(HttpMethod.DELETE,
                    String.format(REMOTE_END_POINT, namespace, appName, encodedKey));
        HttpResponse httpResponse = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
        int responseCode = httpResponse.getResponseCode();
        if (responseCode != 200) {
          String notFoundExceptionMessage = String.format(
              "Namespace %s or application %s not found.", namespace,
              appName);
          handleErrorResponse(responseCode, httpResponse.getResponseBodyAsString(),
              notFoundExceptionMessage);
        }
      }, retryStrategy);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void handleErrorResponse(int responseCode, String responseBody,
      String notFoundExceptionMessage) throws NotFoundException, IOException {
    switch (responseCode) {
      // throw retryable error if service is not available, might be temporary
      case HttpURLConnection.HTTP_BAD_GATEWAY:
      case HttpURLConnection.HTTP_UNAVAILABLE:
      case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
        throw new ServiceUnavailableException(Constants.Service.APP_FABRIC_HTTP,
            Constants.Service.APP_FABRIC_HTTP
                + " service is not available with status " + responseCode);
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw new NotFoundException(notFoundExceptionMessage);
    }
    throw new IOException(
        "Remote call failed with error response: " + responseCode + ": " + responseBody);
  }
}
