/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.runtimejob;

import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.internal.app.runtime.ThrowingRunnable;
import io.cdap.cdap.internal.app.runtime.distributed.AbstractRuntimeTwillController;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Implementation of {@link TwillController} that uses {@link RuntimeJobManager} to monitor and
 * control a running program.
 */
class RuntimeJobTwillController extends AbstractRuntimeTwillController {

  private final RuntimeJobManager jobManager;
  private final ExecutorService executor;
  private final ProgramRunInfo programRunInfo;

  RuntimeJobTwillController(RuntimeJobManager jobManager, ProgramRunId programRunId,
                            CompletionStage<?> startupCompletionStage, ExecutorService executor) {
    super(programRunId, startupCompletionStage);
    this.jobManager = jobManager;
    this.executor = executor;
    this.programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace(programRunId.getNamespace())
      .setApplication(programRunId.getApplication())
      .setVersion(programRunId.getVersion())
      .setProgramType(programRunId.getType().getPrettyName())
      .setProgram(programRunId.getProgram())
      .setRun(programRunId.getRun()).build();
  }

  RuntimeJobManager getJobManager() {
    return jobManager;
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    return terminateJob(() -> jobManager.stop(programRunInfo));
  }

  @Override
  public void kill() {
    try {
      Uninterruptibles.getUninterruptibly(terminateJob(() -> jobManager.kill(programRunInfo)));
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to kill the running program " + programRunInfo, e);
    }
  }

  private CompletableFuture<TwillController> terminateJob(ThrowingRunnable action) {
    if (getTerminationFuture().isDone()) {
      return CompletableFuture.completedFuture(this);
    }

    CompletableFuture<TwillController> result = getTerminationFuture().thenApply(r -> r);
    executor.execute(() -> {
      try {
        if (jobManager.getDetail(programRunInfo).isPresent()) {
          action.run();
        }
        // Complete the termination future. This will in turn complete the result future.
        getTerminationFuture().complete(RuntimeJobTwillController.this);
      } catch (Exception e) {
        // Only fail the result future. We have to keep the terminationFuture to be not completed so that the
        // caller can retry termination.
        result.completeExceptionally(e);
      }
    });
    return result;
  }
}
