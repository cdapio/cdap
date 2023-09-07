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

package io.cdap.cdap.internal.operations;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.operation.OperationRun;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.app.deploy.ProgramRunDispatcher;
import io.cdap.cdap.app.deploy.ProgramRunDispatcherContext;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramController.State;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.internal.remote.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.sourcecontrol.operationrunner.RemoteSourceControlOperationRunner;
import io.cdap.cdap.sourcecontrol.worker.OperationRunnerTask;
import io.cdap.common.http.HttpRequestConfig;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationRunDispatcher implements ProgramRunDispatcher {
  private static final Gson GSON = new GsonBuilder().create();

  private static final Logger LOG = LoggerFactory.getLogger(RemoteSourceControlOperationRunner.class);
  private final RemoteTaskExecutor remoteTaskExecutor;

  @Inject
  OperationRunDispatcher(
      CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory
  ) {
    int readTimeout = cConf.getInt(Constants.TaskWorker.SOURCE_CONTROL_HTTP_CLIENT_READ_TIMEOUT_MS);
    int connectTimeout = cConf.getInt(Constants.TaskWorker.SOURCE_CONTROL_HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    HttpRequestConfig httpRequestConfig = new HttpRequestConfig(connectTimeout, readTimeout, false);
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory,
        RemoteTaskExecutor.Type.TASK_WORKER, httpRequestConfig);
  }
  @Override
  public ProgramController dispatchProgram(ProgramRunDispatcherContext dispatcherContext)
      throws Exception {
    String operationRequest = dispatcherContext.getProgramOptions()
        .getUserArguments().getOption(ProgramOptionConstants.OPERATION_REQUEST);
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(OperationRunnerTask.class.getName())
        .withParam(operationRequest).build();
    remoteTaskExecutor.runTask(request);
    return new OperationController(
        new OperationRun(dispatcherContext.getRunId().getId(), "", "", System.currentTimeMillis()),
        State.ALIVE
    );
  }
}
