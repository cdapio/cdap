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
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.app.deploy.LaunchDispatcher;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.deploy.pipeline.AppLaunchInfo;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.app.worker.LaunchDispatchTask;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class RemoteLaunchDispatcher implements LaunchDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteLaunchDispatcher.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
      .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();
  private final AppLaunchInfo appLaunchInfo;
  private final Store store;
  private final ProgramRunnerFactory programRunnerFactory;
  private final RemoteTaskExecutor remoteTaskExecutor;
  private ProgramRunnerFactory remoteProgramRunnerFactory;
  private TwillRunnerService remoteTwillRunnerService;

  @Inject
  public RemoteLaunchDispatcher(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory, @Assisted AppLaunchInfo appLaunchInfo,
      Store store, ProgramRunnerFactory programRunnerFactory) {
    this.appLaunchInfo = appLaunchInfo;
    this.store = store;
    this.programRunnerFactory = programRunnerFactory;
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService,
        remoteClientFactory);
  }

  /**
   * Optional guice injection for the {@link ProgramRunnerFactory} used for remote execution. It is
   * optional because in unit-test we don't have need for that.
   */
  @Inject(optional = true)
  void setRemoteProgramRunnerFactory(
      @Constants.AppFabric.RemoteExecution ProgramRunnerFactory runnerFactory) {
    this.remoteProgramRunnerFactory = runnerFactory;
  }

  /**
   * Optional guice injection for the {@link TwillRunnerService} used for remote execution. It is
   * optional because in unit-test we don't have need for that.
   */
  @Inject(optional = true)
  void setRemoteTwillRunnerService(
      @Constants.AppFabric.RemoteExecution TwillRunnerService twillRunnerService) {
    this.remoteTwillRunnerService = twillRunnerService;
  }

  @Override
  public ProgramController dispatchPipelineLaunch() throws Exception {
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(LaunchDispatchTask.class.getName())
        .withParam(GSON.toJson(appLaunchInfo)).build();
    byte[] result = remoteTaskExecutor.runTask(request);
    LaunchDispatchResponse response = GSON.fromJson(new String(result, StandardCharsets.UTF_8),
        LaunchDispatchResponse.class);
    if (!response.isSuccessfulLaunch()) {
      throw new Exception("Failed");
    }
    ProgramId programId = appLaunchInfo.getProgramDescriptor().getProgramId();
    ProgramRunId programRunId = programId.run(appLaunchInfo.getRunId());
    ProgramRunner runner = (
        ProgramRunners.getClusterMode(appLaunchInfo.getProgramOptions()) == ClusterMode.ON_PREMISE
            ? programRunnerFactory
            : Optional.ofNullable(remoteProgramRunnerFactory)
                .orElseThrow(UnsupportedOperationException::new))
        .create(programId.getType());
    RunRecordDetail runRecordDetail = store.getRun(programRunId);
    LOG.debug("RunRecordDetail: {}", runRecordDetail);
    LOG.debug("TwillRunnerService: {}", remoteTwillRunnerService);
    TwillController twillController = null;
    if (remoteTwillRunnerService instanceof RemoteExecutionTwillRunnerService
        && runRecordDetail != null) {
      LOG.debug("Creating TwillController");
      twillController = ((RemoteExecutionTwillRunnerService) remoteTwillRunnerService)
          .createTwillControllerFromRunRecord(runRecordDetail);
      LOG.debug("TwillController: {}", twillController);
    }
    ProgramController programController = null;
    if (runner instanceof DistributedWorkflowProgramRunner && twillController != null) {
      LOG.debug("Creating ProgramController");
      programController = ((DistributedWorkflowProgramRunner) runner)
          .createProgramController(programRunId, twillController);
      LOG.debug("ProgramController: {}", programController);
    }
    return programController;
  }
}
