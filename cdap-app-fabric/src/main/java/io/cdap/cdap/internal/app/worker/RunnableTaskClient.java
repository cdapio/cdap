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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.provision.task.RemoteProvisioningSubtask;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for submitting {@link RunnableTaskRequest} to {@link TaskWorkerService}.
 */
public class RunnableTaskClient {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteProvisioningSubtask.class);
  private static final Gson GSON = new Gson();
  private final RemoteClient remoteClient;

  public RunnableTaskClient(DiscoveryServiceClient discoveryServiceClient) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
                                         new DefaultHttpRequestConfig(false),
                                         String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));
  }

  //TODO implement retry with backoff mechanism
  public String submitTask(RunnableTaskRequest runnableTaskRequest) throws Exception {
    String reqBody = GSON.toJson(runnableTaskRequest);

    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(
      HttpMethod.POST, String.format("/worker/run")).withBody(reqBody);

    HttpRequest request = requestBuilder.build();
    HttpResponse response = remoteClient.execute(request);

    return response.getResponseBodyAsString();
  }

}
