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
import io.cdap.cdap.internal.app.dispatcher.ProvisionerRunnableTask;
import io.cdap.cdap.internal.app.runtime.codec.CConfigurationCodec;
import io.cdap.cdap.internal.app.worker.RunnableTaskClient;
import io.cdap.cdap.internal.app.worker.RunnableTaskRequest;
import io.cdap.cdap.internal.provision.ProvisioningOp;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

/**
 * Provisioning subtask that executes remotely by {@link io.cdap.cdap.internal.app.worker.TaskWorkerService}.
 */
public class RemoteProvisioningSubtask extends ProvisioningSubtask {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteProvisioningSubtask.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(CConfiguration.class, new CConfigurationCodec()).create();
  private final RunnableTaskClient runnableTaskClient;

  public RemoteProvisioningSubtask(Provisioner provisioner, ProvisionerContext provisionerContext,
                                   Function<Cluster, Optional<ProvisioningOp.Status>> transition,
                                   DiscoveryServiceClient discoveryServiceClient) {
    super(provisioner, provisionerContext, transition);
    runnableTaskClient = new RunnableTaskClient(discoveryServiceClient);

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
      RunnableTaskRequest req = new RunnableTaskRequest(ProvisionerRunnableTask.class.getName(), param, false);
      jsonResponse = runnableTaskClient.submitTask(req);
    } catch (Exception ex) {
      LOG.warn(String.format("Exception caught during RemoteClusterCreateSubtask.execute, got json response: %s",
                             jsonResponse),
               ex);
    }
    return GSON.fromJson(jsonResponse, Cluster.class);
  }
}
