/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.app.deploy.ProgramRunDispatcher;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramControllerCreator;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.AppFabric.RemoteExecution;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.deploy.pipeline.ProgramRunDispatcherInfo;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.app.worker.ProgramRunDispatcherTask;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Implementation of {@link ProgramRunDispatcher} which enables Program-run execution to take place remotely (E.g.- in
 * another service)
 */
public class RemoteProgramRunDispatcher implements ProgramRunDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteProgramRunDispatcher.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
      new GsonBuilder())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
    .registerTypeAdapter(Requirements.class, new RequirementsCodec())
    .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();
  private final Store store;
  private final ProgramRunnerFactory programRunnerFactory;
  private final RemoteTaskExecutor remoteTaskExecutor;
  private final ProgramRunnerFactory remoteProgramRunnerFactory;
  private final RemoteExecutionTwillRunnerService remoteTwillRunnerService;

  @Inject
  public RemoteProgramRunDispatcher(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                    RemoteClientFactory remoteClientFactory, Store store,
                                    ProgramRunnerFactory programRunnerFactory,
                                    @RemoteExecution ProgramRunnerFactory remoteProgramRunnerFactory,
                                    @RemoteExecution TwillRunnerService twillRunnerService) {
    this.store = store;
    this.programRunnerFactory = programRunnerFactory;
    this.remoteProgramRunnerFactory = remoteProgramRunnerFactory;
    // TODO(CDAP-18964): Get rid of type casting.
    this.remoteTwillRunnerService = (RemoteExecutionTwillRunnerService) twillRunnerService;
    int connectTimeout = cConf.getInt(Constants.SystemWorker.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    int readTimeout = cConf.getInt(Constants.SystemWorker.HTTP_CLIENT_READ_TIMEOUT_MS);
    HttpRequestConfig httpRequestConfig = new HttpRequestConfig(connectTimeout, readTimeout, false);
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService,
                                                     remoteClientFactory, RemoteTaskExecutor.Type.SYSTEM_WORKER,
                                                     httpRequestConfig);
  }

  @Override
  public ProgramController dispatchProgram(ProgramRunDispatcherInfo programRunDispatcherInfo) throws Exception {
    RunId runId = programRunDispatcherInfo.getRunId();
    LOG.debug("Dispatching Program Run operation for Run ID: {}", runId.getId());
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(ProgramRunDispatcherTask.class.getName())
      .withParam(GSON.toJson(programRunDispatcherInfo)).build();
    remoteTaskExecutor.runTask(request);
    ProgramId programId = programRunDispatcherInfo.getProgramDescriptor().getProgramId();
    ProgramRunId programRunId = programId.run(runId);
    ProgramRunner runner =
      (ProgramRunners.getClusterMode(programRunDispatcherInfo.getProgramOptions()) == ClusterMode.ON_PREMISE
        ? programRunnerFactory
        : Optional.ofNullable(remoteProgramRunnerFactory).orElseThrow(UnsupportedOperationException::new)).create(
        programId.getType());
    if (!(runner instanceof ProgramControllerCreator)) {
      String msg = String.format("Program %s with runid %s uses an unsupported controller for remote dispatching.",
                                 programRunDispatcherInfo.getProgramDescriptor().getProgramId(),
                                 programRunDispatcherInfo.getRunId());
      throw new UnsupportedOperationException(msg);
    }

    RunRecordDetail runRecordDetail = store.getRun(programRunId);
    if (runRecordDetail == null) {
      String msg = String.format("Could not find run record for Program %s with runid %s",
                                 programRunDispatcherInfo.getProgramDescriptor().getProgramId(),
                                 programRunDispatcherInfo.getRunId());
      throw new IllegalStateException(msg);
    }
    TwillController twillController = remoteTwillRunnerService.createTwillControllerFromRunRecord(runRecordDetail);
    ProgramController programController = null;
    if (twillController != null) {
      programController = ((ProgramControllerCreator) runner).createProgramController(programRunId, twillController);
    }
    if (programController == null) {
      String msg = String.format("Unable to create controller for Program %s with runid %s",
                                 programRunDispatcherInfo.getProgramDescriptor().getProgramId(),
                                 programRunDispatcherInfo.getRunId());
      throw new Exception(msg);
    }
    return programController;
  }
}
