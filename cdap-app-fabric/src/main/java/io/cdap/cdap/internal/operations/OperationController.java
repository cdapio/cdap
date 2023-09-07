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

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.api.service.operation.OperationRun;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;

public class OperationController implements ProgramController {
  private final OperationRun operationRun;
  private final AtomicReference<State> state;

  public OperationController(OperationRun operationRun, @Nullable State state) {
    this.state = new AtomicReference<>(State.STARTING);
    if (state != null) {
      this.state.set(state);
    }
    this.operationRun = operationRun;
  }

  @Override
  public ProgramRunId getProgramRunId() {

    return new ProgramRunId(operationRun.getNamespace(), operationRun.getOperationType(),
        ProgramType.Operation, operationRun.getOperationId(), "");
  }

  @Override
  public RunId getRunId() {
    return RunIds.fromString(operationRun.getOperationId());
  }

  @Override
  public ListenableFuture<ProgramController> suspend() {
    // no-op
    return null;
  }

  @Override
  public ListenableFuture<ProgramController> resume() {
    // no-op
    return null;
  }

  @Override
  public ListenableFuture<ProgramController> stop() {
    // no-op stop is done by the operation runner
    return null;
  }

  @Override
  public ListenableFuture<ProgramController> stop(long timeout, TimeUnit timeoutUnit) {
    // no-op stop is done by the operation runner
    return null;
  }

  @Override
  public void kill() {
    stop();
  }

  @Override
  public State getState() {
    return this.state.get();
  }

  @Override
  public Throwable getFailureCause() {
    return null;
  }

  @Override
  public Cancellable addListener(Listener listener, Executor executor) {
    return null;
  }

  @Override
  public ListenableFuture<ProgramController> command(String name, Object value) {
    // no-op stop is done by the operation runner
    return null;
  }
}
