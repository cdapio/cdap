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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.concurrent.CompletableFuture;

final class FireAndForgetControllerFactory extends ControllerFactory {

  private final RemoteExecutionTwillRunnerService remoteExecutionTwillRunnerService;

  FireAndForgetControllerFactory(RemoteExecutionTwillRunnerService remoteExecutionTwillRunnerService,
                                 ProgramRunId programRunId, ProgramOptions programOpts) {
    super(remoteExecutionTwillRunnerService, programRunId, programOpts);
    this.remoteExecutionTwillRunnerService = remoteExecutionTwillRunnerService;
  }

  @Override
  protected ExecutionService createExecutionService(ProgramRunId programRunId,
                                                    ProgramOptions programOpts,
                                                    RemoteProcessController processController) {
    return new NoopExecutionService(remoteExecutionTwillRunnerService.getcConf(),
                                    programRunId,
                                    remoteExecutionTwillRunnerService.getScheduler(),
                                    processController,
                                    remoteExecutionTwillRunnerService.getProgramStateWriter());
  }

  @Override
  protected RemoteExecutionTwillController createController(ProgramRunId programRunId, ProgramOptions programOpts,
                                                            RemoteProcessController processController,
                                                            CompletableFuture<Void> startupTaskCompletion)
    throws Exception {
    ExecutionService remoteExecutionService = createRemoteExecutionService(programRunId, programOpts,
                                                                           processController);

    // Create the controller and start the runtime monitor when the startup task completed successfully.
    RemoteExecutionTwillController controller =
      new RemoteExecutionTwillController(remoteExecutionTwillRunnerService.getcConf(),
                                         programRunId,
                                         startupTaskCompletion,
                                         processController,
                                         remoteExecutionTwillRunnerService.getScheduler(), remoteExecutionService);

    controller.release();
    return controller;
  }
}
