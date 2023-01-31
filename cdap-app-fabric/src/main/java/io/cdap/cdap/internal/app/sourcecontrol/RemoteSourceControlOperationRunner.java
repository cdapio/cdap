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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.worker.SourceControlPullAppTask;
import io.cdap.cdap.internal.app.worker.SourceControlPushAppsTask;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.operationrunner.AppDetailsToPush;
import io.cdap.cdap.sourcecontrol.operationrunner.ListAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PullApplicationContext;
import io.cdap.cdap.sourcecontrol.operationrunner.PushApplicationsContext;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RemoteSourceControlOperationRunner implements SourceControlOperationRunner {

  private static final Gson GSON = new GsonBuilder().create();
  private static final Logger LOG = LoggerFactory.getLogger(RemoteSourceControlOperationRunner.class);

  private final RemoteTaskExecutor remoteTaskExecutor;
  private final CConfiguration cConf;
  private final RepositoryConfig repositoryConfig;

  @Inject
  public RemoteSourceControlOperationRunner(CConfiguration cConf,
                                            MetricsCollectionService metricsCollectionService,
                                            RemoteClientFactory remoteClientFactory,
                                            @Assisted RepositoryConfig repositoryConfig) {
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory,
                                                     RemoteTaskExecutor.Type.TASK_WORKER);
    this.cConf = cConf;
    this.repositoryConfig = repositoryConfig;
  }

  @Override
  public ListenableFuture<PushAppsResponse> push(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws IOException {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(SourceControlPushAppsTask.class.getName())
        .withParam(GSON.toJson(new PushApplicationsContext(appsToPush, commitDetails, repositoryConfig)))
        .build();

      byte[] result = remoteTaskExecutor.runTask(request);
      return Futures.immediateFuture(GSON.fromJson(new String(result, StandardCharsets.UTF_8),
                                                   PushAppsResponse.class));
    } catch (Exception ex) {
      return Futures.immediateFailedFuture(ex);
    }
  }

  @Override
  public ListenableFuture<PullAppResponse> pull(String applicationName, String branchName) throws IOException {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(SourceControlPullAppTask.class.getName())
        .withParam(GSON.toJson(new PullApplicationContext(applicationName, branchName, repositoryConfig)))
        .build();

      byte[] result = remoteTaskExecutor.runTask(request);
      return Futures.immediateFuture(GSON.fromJson(new String(result, StandardCharsets.UTF_8),
                                                   PullAppResponse.class));
    } catch (Exception ex) {
      return Futures.immediateFailedFuture(ex);
    }
  }

  @Override
  public List<ListAppResponse> list() {
    return null;
  }
}
