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

import com.google.inject.Inject;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;

/**
 * Implementation of {@link TwillControllerCreator} for Isolated(Remote) Cluster mode.
 */
public class RemoteTwillControllerCreator implements TwillControllerCreator {

  private final RemoteExecutionTwillRunnerService remoteExecutionTwillRunnerService;
  private final Store store;

  @Inject
  RemoteTwillControllerCreator(@Constants.AppFabric.RemoteExecution TwillRunnerService twillRunnerService,
                               Store store) {
    this.remoteExecutionTwillRunnerService = (RemoteExecutionTwillRunnerService) twillRunnerService;
    this.store = store;
  }

  @Override
  public TwillController createTwillController(ProgramRunId programRunId) {
    RunRecordDetail runRecordDetail = store.getRun(programRunId);
    if (runRecordDetail == null) {
      String msg = String.format("Could not find run record for Program %s with runid %s", programRunId.getProgram(),
                                 programRunId.getRun());
      throw new IllegalStateException(msg);
    }
    return remoteExecutionTwillRunnerService.createTwillControllerFromRunRecord(runRecordDetail);
  }
}
