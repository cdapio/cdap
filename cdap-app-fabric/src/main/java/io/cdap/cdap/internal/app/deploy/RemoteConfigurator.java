/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.deploy;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.worker.ConfiguratorTask;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

import java.nio.charset.StandardCharsets;

/**
 * RemoteConfigurator sends a request to another {@link Configurator} running remotely.
 */
public class RemoteConfigurator implements Configurator {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
    .registerTypeAdapter(Requirements.class, new RequirementsCodec())
    .create();

  private final AppDeploymentInfo deploymentInfo;
  private final RemoteTaskExecutor remoteTaskExecutor;

  @Inject
  public RemoteConfigurator(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                            @Assisted AppDeploymentInfo deploymentInfo,
                            RemoteClientFactory remoteClientFactory) {
    this.deploymentInfo = deploymentInfo;
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory);
  }

  @Override
  public ListenableFuture<ConfigResponse> config() {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(ConfiguratorTask.class.getName())
        .withParam(GSON.toJson(deploymentInfo))
        .build();

      byte[] result = remoteTaskExecutor.runTask(request);
      return Futures.immediateFuture(GSON.fromJson(new String(result, StandardCharsets.UTF_8),
                                                   DefaultConfigResponse.class));
    } catch (Exception ex) {
      return Futures.immediateFailedFuture(ex);
    }
  }
}
