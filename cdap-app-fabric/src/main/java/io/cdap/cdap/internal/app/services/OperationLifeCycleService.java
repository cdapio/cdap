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

package io.cdap.cdap.internal.app.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.internal.remote.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.sourcecontrol.operationrunner.RemoteSourceControlOperationRunner;
import io.cdap.common.http.HttpRequestConfig;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationLifeCycleService {
  private static final Gson GSON = new GsonBuilder().create();

  private static final Logger LOG = LoggerFactory.getLogger(RemoteSourceControlOperationRunner.class);
  private final RemoteTaskExecutor remoteTaskExecutor;

  @Inject
  OperationLifeCycleService(
      CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory) {
    int readTimeout = cConf.getInt(Constants.TaskWorker.SOURCE_CONTROL_HTTP_CLIENT_READ_TIMEOUT_MS);
    int connectTimeout = cConf.getInt(Constants.TaskWorker.SOURCE_CONTROL_HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    HttpRequestConfig httpRequestConfig = new HttpRequestConfig(connectTimeout, readTimeout, false);
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory,
        RemoteTaskExecutor.Type.TASK_WORKER, httpRequestConfig);
  }

  public void startOperation(ProgramOptions options, ProgramRunId programRunId) throws Exception{
      // start the operation here
  }

  public void startOperation(RunRecordDetail runRecordDetail) throws Exception{
    // start the operation here
  }

}
