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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.task.RunnableTaskRequest;
import io.cdap.cdap.api.task.RunnableTaskResponse;
import io.cdap.cdap.api.task.TaskWorkerHandler;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * RemoteTaskWorkerHandler
 */
public class RemoteTaskWorkerHandler implements TaskWorkerHandler {

  private final RemoteClient remoteClient;
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(RemoteTaskWorkerHandler.class);

  @Inject
  RemoteTaskWorkerHandler(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(
      discoveryClient, Constants.Service.TASK_WORKER,
      new DefaultHttpRequestConfig(false), Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  @Override
  public RunnableTaskResponse<?> runTask(RunnableTaskRequest runnableTaskRequest) throws IOException {
    String url = "/worker/run";
    //LOG.info("RemoteTaskWorkerHandler *** Building url");
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.POST, url);
    HttpRequest httpRequest = requestBuilder.withBody(GSON.toJson(runnableTaskRequest)).build();
    //LOG.info("RemoteTaskWorkerHandler got request " + httpRequest);
    HttpResponse httpResponse = remoteClient.execute(httpRequest);
    if (httpResponse.getResponseCode() != 200) {
      return new RunnableTaskResponse<>(
        "Got response code " + httpResponse.getResponseCode() + " . Error message is \n" + httpResponse
          .getResponseBodyAsString());
    }
    return new RunnableTaskResponse<>(httpResponse.getResponseBodyAsString());
  }
}
