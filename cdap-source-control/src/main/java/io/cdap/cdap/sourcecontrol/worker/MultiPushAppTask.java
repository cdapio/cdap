/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.worker;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPushAppOperationRequest;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The task that pushes an application to linked repository.
 */
public class MultiPushAppTask extends SourceControlTask {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(MultiPushAppTask.class);

  @Inject
  MultiPushAppTask(CConfiguration cConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService
  ) {
    super(cConf, discoveryService, discoveryServiceClient,
        metricsCollectionService);
  }

  @Override
  public void doRun(RunnableTaskContext context)
    throws AuthenticationConfigException, NoChangesToPushException, IOException {
    LOG.warn(context.getParam());
    MultiPushAppOperationRequest operationRequest =
        GSON.fromJson(context.getParam(), MultiPushAppOperationRequest.class);
    RemoteClient remoteClient = remoteClientFactory.createRemoteClient(
        Constants.Service.APP_FABRIC_HTTP,
        RemoteClientFactory.NO_VERIFY_HTTP_REQUEST_CONFIG,
        Gateway.API_VERSION_3
    );
    inMemoryOperationRunner.multipush(
      operationRequest, null, remoteClient
    );
  }
}
