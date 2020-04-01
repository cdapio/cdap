/*
 * Copyright © 2018 Cask Data, Inc.
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

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.internal.app.runtime.distributed.AbstractRuntimeTwillController;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

/**
 * Implementation of {@link TwillController} that uses {@link RuntimeMonitor} to monitor and control a running
 * program.
 */
class RemoteExecutionTwillController extends AbstractRuntimeTwillController {

  private final RuntimeMonitor runtimeMonitor;

  RemoteExecutionTwillController(ProgramRunId programRunId,
                                 CompletionStage<?> startupCompletionStage, RuntimeMonitor runtimeMonitor) {
    super(programRunId, startupCompletionStage);
    this.runtimeMonitor = runtimeMonitor;
    runtimeMonitor.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        getTerminationFuture().complete(RemoteExecutionTwillController.this);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        getTerminationFuture().completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Returns the {@link RuntimeMonitor} used by this controller.
   */
  RuntimeMonitor getRuntimeMonitor() {
    return runtimeMonitor;
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    try {
      runtimeMonitor.requestStop();
    } catch (Exception e) {
      throw new RuntimeException("Failed when requesting program " + getProgramRunId() + " to stop", e);
    }
    return getTerminationFuture();
  }
}
