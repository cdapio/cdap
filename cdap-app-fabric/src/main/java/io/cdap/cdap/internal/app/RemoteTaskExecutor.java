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

package io.cdap.cdap.internal.app;

import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.io.ExposedByteArrayOutputStream;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Helper class for executing a {@link RunnableTaskRequest} on a remote worker.
 */
public class RemoteTaskExecutor {

  private static final Gson GSON = new Gson();
  private static final String TASK_WORKER_URL = "/worker/run";

  private final boolean compression;
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;

  public RemoteTaskExecutor(CConfiguration cConf, RemoteClientFactory remoteClientFactory) {
    this.compression = cConf.getBoolean(Constants.TaskWorker.COMPRESSION_ENABLED);
    this.remoteClient = remoteClientFactory.createRemoteClient(Constants.Service.TASK_WORKER,
                                                               new DefaultHttpRequestConfig(false),
                                                               Constants.Gateway.INTERNAL_API_VERSION_3);
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, Constants.Service.TASK_WORKER + ".");
  }

  /**
   * Sends the {@link RunnableTaskRequest} to a remote worker and returns the result.
   * Retries sending the request if the workers are busy.
   *
   * @param runnableTaskRequest {@link RunnableTaskRequest} with details of task
   * @return byte[] response from remote task
   * @throws Exception returned by remote task if any
   */
  public byte[] runTask(RunnableTaskRequest runnableTaskRequest) throws Exception {
    ByteBuffer requestBody = encodeTaskRequest(runnableTaskRequest);

    return Retries.callWithRetries(() -> {
      try {
        HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.POST, TASK_WORKER_URL)
          .withBody(requestBody.duplicate());
        if (compression) {
          requestBuilder.addHeader(HttpHeaders.CONTENT_ENCODING, "gzip");
          requestBuilder.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate");
        }
        HttpRequest httpRequest = requestBuilder.build();

        HttpResponse httpResponse = remoteClient.execute(httpRequest);
        if (httpResponse.getResponseCode() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
          throw new RetryableException(
            String.format("Received response code %s for %s", httpResponse.getResponseCode(),
                          runnableTaskRequest.getClassName()));
        }
        if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
          BasicThrowable basicThrowable = GSON
            .fromJson(new String(getResponseBody(httpResponse)), BasicThrowable.class);
          throw RemoteExecutionException.fromBasicThrowable(basicThrowable);
        }
        return getResponseBody(httpResponse);
      } catch (NoRouteToHostException e) {
        throw new RetryableException(
          String.format("Received exception %s for %s", e.getMessage(), runnableTaskRequest.getClassName()));
      }
    }, retryStrategy);
  }

  private ByteBuffer encodeTaskRequest(RunnableTaskRequest request) throws IOException {
    ExposedByteArrayOutputStream bos = new ExposedByteArrayOutputStream();
    try (Writer writer = new OutputStreamWriter(compression ? new GZIPOutputStream(bos) : bos,
                                                StandardCharsets.UTF_8)) {
      GSON.toJson(request, writer);
    }
    return bos.toByteBuffer();
  }

  /**
   * Decodes and return the response body based on the content encoding.
   */
  private byte[] getResponseBody(HttpResponse response) throws IOException {
    String encoding = response.getHeaders().entries().stream()
      .filter(e -> HttpHeaders.CONTENT_ENCODING.equalsIgnoreCase(e.getKey()))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(null);

    if (encoding == null) {
      return response.getResponseBody();
    }

    if ("gzip".equalsIgnoreCase(encoding)) {
      try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(response.getResponseBody()))) {
        return ByteStreams.toByteArray(is);
      }
    }
    if ("deflate".equalsIgnoreCase(encoding)) {
      try (InputStream is = new DeflaterInputStream(new ByteArrayInputStream(response.getResponseBody()))) {
        return ByteStreams.toByteArray(is);
      }
    }

    throw new IllegalArgumentException("Unsupported content encoding " + encoding);
  }
}
