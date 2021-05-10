/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.provision.task;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.dispatcher.ProvisionerRunnableTask;
import io.cdap.cdap.internal.app.dispatcher.RunnableTaskRequest;
import io.cdap.cdap.internal.app.runtime.codec.CConfigurationCodec;
import io.cdap.cdap.internal.provision.ProvisioningOp;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

/**
 * Provisioning subtask that creates a cluster.
 */
public class RemoteClusterCreateSubtask extends ProvisioningSubtask {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteClusterCreateSubtask.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(CConfiguration.class, new CConfigurationCodec()).create();
  private final RemoteClient remoteClientInternal;

  public RemoteClusterCreateSubtask(Provisioner provisioner, ProvisionerContext provisionerContext,
                                    Function<Cluster, Optional<ProvisioningOp.Status>> transition,
                                    DiscoveryServiceClient discoveryServiceClient) {
    super(provisioner, provisionerContext, transition);
    LOG.error("In the remote cluster create subtask constructor");
    this.remoteClientInternal = null; //new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
    //                                                 new DefaultHttpRequestConfig(false),
    //                                                 String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));
  }

  @Override
  public Cluster execute(Cluster cluster) throws Exception {
    LOG.error("In the remote cluster create subtask execute");
    SubtaskConfig config = new SubtaskConfig(provisioner, provisionerContext);
    LOG.error("created config: ");
    LOG.error("ADAPTER:" + GSON.getAdapter(CConfiguration.class).toString());
    String jsonResponse = "";
    try {
      String param = GSON.toJson(config);
      LOG.error("Got config: " + param);
      RunnableTaskRequest req = new RunnableTaskRequest(ProvisionerRunnableTask.class.getName(), param);
      String reqBody = GSON.toJson(req);

      HttpRequest.Builder requestBuilder = remoteClientInternal.requestBuilder(
        HttpMethod.POST, String.format("/worker/run")).withBody(reqBody);

      HttpRequest request = requestBuilder.build();
      LOG.error("sending request to URL: " + request.getURL().toString());
      HttpResponse response = remoteClientInternal.execute(request);

      jsonResponse = response.getResponseBodyAsString();
    } catch (Exception ex) {
      LOG.warn(String.format("Exception caught during RemoteClusterCreateSubtask.execute, got json response: %s",
                             jsonResponse),
               ex);
    }
    return GSON.fromJson(jsonResponse, Cluster.class);
  }
}
