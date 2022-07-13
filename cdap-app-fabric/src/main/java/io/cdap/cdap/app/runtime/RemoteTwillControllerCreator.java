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

package io.cdap.cdap.app.runtime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.RemoteProgramRunDispatcher;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteTwillControllerCreator implements TwillControllerCreator {

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

  private final RemoteExecutionTwillRunnerService remoteExecutionTwillRunnerService;
  private final Store store;

  @Inject
  RemoteTwillControllerCreator(@Constants.AppFabric.RemoteExecution TwillRunnerService twillRunnerService,
                               Store store) {
    this.remoteExecutionTwillRunnerService = (RemoteExecutionTwillRunnerService) twillRunnerService;
    this.store = store;
  }

  @Override
  public TwillController createTwillController(ProgramRunId programRunId, String twillRunId) {
    RunRecordDetail runRecordDetail = store.getRun(programRunId);
    if (runRecordDetail == null) {
      String msg = String.format("Could not find run record for Program %s with runid %s", programRunId.getProgram(),
                                 programRunId.getRun());
      throw new IllegalStateException(msg);
    }
    LOG.debug("RunRecordDetail: {}", runRecordDetail);
    try {
      LOG.debug("RunRecordDetail json: {}", GSON.toJson(runRecordDetail));
    } catch (Exception e) {
      LOG.error("Error", e);
    }
    return remoteExecutionTwillRunnerService.createTwillControllerFromRunRecord(runRecordDetail);
  }
}
