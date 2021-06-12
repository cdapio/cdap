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

import com.google.gson.Gson;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RemoteTaskException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;

/**
 * Helper class for executing a {@link RunnableTaskRequest} on a remote worker.
 */
public class RemoteTaskExecutor {

  private static final Gson GSON = new Gson();
  private static final String TASK_WORKER_URL = "/worker/run";
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;

  public RemoteTaskExecutor(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
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
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.POST, TASK_WORKER_URL);
    HttpRequest httpRequest = requestBuilder.withBody(GSON.toJson(runnableTaskRequest)).build();
    return Retries.callWithRetries(() -> {
      try {
        HttpResponse httpResponse = remoteClient.execute(httpRequest);
        if (httpResponse.getResponseCode() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
          throw new RetryableException(
            String.format("Received response code %s for %s", httpResponse.getResponseCode(),
                          runnableTaskRequest.getClassName()));
        }
        if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
          BasicThrowable basicThrowable = GSON
            .fromJson(new String(httpResponse.getResponseBody()), BasicThrowable.class);
          throw createExceptionObject(basicThrowable);
        }
        return httpResponse.getResponseBody();
      } catch (NoRouteToHostException e) {
        throw new RetryableException(
          String.format("Received exception %s for %s", e.getMessage(), runnableTaskRequest.getClassName()));
      }
    }, retryStrategy);
  }

  private Exception createExceptionObject(BasicThrowable basicThrowable) {
    BasicThrowable cause = basicThrowable.getCause();
    Exception causeException = cause == null ? null : createExceptionObject(cause);
    RemoteTaskException remoteTaskException = new RemoteTaskException(basicThrowable.getClassName(),
                                                                      basicThrowable.getMessage(), causeException);
    remoteTaskException.setStackTrace(basicThrowable.getStackTraces());

    // Wrap the remote exception as the cause so that we retain the local stacktrace of the exception.
    return new RemoteExecutionException(remoteTaskException);
  }
}
