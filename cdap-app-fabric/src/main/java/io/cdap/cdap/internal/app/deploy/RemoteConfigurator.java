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
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ConfiguratorConfig;
import io.cdap.cdap.internal.app.worker.ConfigResponseResult;
import io.cdap.cdap.internal.app.worker.ConfiguratorTask;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteConfigurator sends a request to another {@link Configurator} running remotely.
 */
public class RemoteConfigurator implements Configurator {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteConfigurator.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final ConfiguratorConfig config;

  private final RemoteClient remoteClientInternal;

  @Inject
  public RemoteConfigurator(DiscoveryServiceClient discoveryServiceClient, @Assisted AppDeploymentInfo deploymentInfo) {
    this.config = new ConfiguratorConfig(deploymentInfo);
    this.remoteClientInternal = new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
                                                 new DefaultHttpRequestConfig(false),
                                                 Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  public RemoteConfigurator(NamespaceId appNamespace, ArtifactId artifactId,
                            String appClassName, String applicationName, String applicationVersion, String configString,
                            DiscoveryServiceClient discoveryServiceClient, Location artifactLocation) {
    this.config = new ConfiguratorConfig(appNamespace,
                                         artifactId, appClassName,
                                         applicationName,
                                         applicationVersion,
                                         configString, artifactLocation.toURI());

    this.remoteClientInternal = new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
                                                 new DefaultHttpRequestConfig(false),
                                                 Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  @Override
  public ListenableFuture<ConfigResponse> config() {

    String jsonResponse = "";

    try {
      String param = GSON.toJson(config);
      RunnableTaskRequest req = RunnableTaskRequest.getBuilder(ConfiguratorTask.class.getName()).withParam(param)
        .build();
      String reqBody = GSON.toJson(req);

      HttpRequest.Builder requestBuilder = remoteClientInternal.requestBuilder(
        HttpMethod.POST, "/worker/run").withBody(reqBody);
      HttpResponse response = remoteClientInternal.execute(requestBuilder.build());

      jsonResponse = response.getResponseBodyAsString();
    } catch (Exception ex) {
      LOG.warn("Exception caught during RemoteConfigurator.config, got json response: {} with error {}", jsonResponse,
               ex);
      return Futures.immediateFailedFuture(ex);
    }

    ConfigResponseResult configResponse = GSON.fromJson(jsonResponse, ConfigResponseResult.class);
    if (configResponse.getException() != null) {
      //TODO: Refactor to use RemoteTaskExecutor once PR #13383 is merged
      Exception exception = new Exception(configResponse.getException().getMessage());
      exception.setStackTrace(configResponse.getException().getStackTraces());
      return Futures.immediateFailedFuture(exception);
    }
    return Futures.immediateFuture(configResponse.getConfigResponse());
  }
}
