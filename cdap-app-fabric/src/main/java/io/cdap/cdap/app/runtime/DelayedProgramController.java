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

package io.cdap.cdap.app.runtime;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.lang.Delegator;
import io.cdap.cdap.internal.app.runtime.AbstractProgramController;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A {@link ProgramController} that delegates all methods to another {@link ProgramController}, in which can be
 * set later. All method calls on this instance should be non-blocking.
 */
public final class DelayedProgramController implements ProgramController, Delegator<ProgramController> {

  private final ProgramRunId programRunId;
  private final RunId runId;
  private final CompletableFuture<ProgramController> delegateFuture;

  DelayedProgramController(ProgramRunId programRunId) {
    this.programRunId = programRunId;
    this.runId = RunIds.fromString(programRunId.getRun());
    this.delegateFuture = new CompletableFuture<>();
  }

  /**
   * Sets the delegate {@link ProgramController} to the one provided.
   */
  void setProgramController(ProgramController controller) {
    delegateFuture.complete(controller);
  }

  /**
   * Force this program controller into ERROR state if the delegating program controller hasn't been set.
   */
  void failed(Throwable t) {
    delegateFuture.complete(new FailedProgramController(programRunId, t));
  }

  /**
   * Returns the {@link ProgramController} being delegated to. This method will block until the delegates is available.
   */
  @Override
  public ProgramController getDelegate() {
    try {
      return Uninterruptibles.getUninterruptibly(delegateFuture);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public ListenableFuture<ProgramController> suspend() {
    return callDelegate(ProgramController::suspend);
  }

  @Override
  public ListenableFuture<ProgramController> resume() {
    return callDelegate(ProgramController::resume);
  }

  @Override
  public ListenableFuture<ProgramController> stop() {
    return callDelegate(ProgramController::stop);
  }

  @Override
  public State getState() {
    try {
      return Uninterruptibles.getUninterruptibly(delegateFuture).getState();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Throwable getFailureCause() {
    try {
      return Uninterruptibles.getUninterruptibly(delegateFuture).getFailureCause();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Cancellable addListener(Listener listener, Executor executor) {
    CompletableFuture<Cancellable> cancellableFuture = new CompletableFuture<>();
    delegateFuture.whenComplete((programController, throwable) -> {
      if (throwable == null) {
        cancellableFuture.complete(programController.addListener(listener, executor));
      } else {
        cancellableFuture.completeExceptionally(throwable);
      }
    });
    return () -> {
      try {
        Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public ListenableFuture<ProgramController> command(String name, Object value) {
    return callDelegate(controller -> controller.command(name, value));
  }

  /**
   * Makes a method call on the delegating {@link ProgramController} when it is available.
   */
  private ListenableFuture<ProgramController> callDelegate(
    Function<ProgramController, ListenableFuture<ProgramController>> callFunc
  ) {
    SettableFuture<ProgramController> resultFuture = SettableFuture.create();
    delegateFuture.whenComplete((programController, throwable) -> {
      if (throwable != null) {
        resultFuture.setException(throwable);
      }
      Futures.addCallback(callFunc.apply(programController), new FutureCallback<ProgramController>() {
        @Override
        public void onSuccess(ProgramController result) {
          resultFuture.set(result);
        }

        @Override
        public void onFailure(Throwable t) {
          resultFuture.setException(t);
        }
      });
    });
    return resultFuture;
  }

  /**
   * A {@link ProgramController} that is in error state.
   */
  private static final class FailedProgramController extends AbstractProgramController {

    FailedProgramController(ProgramRunId programRunId, Throwable t) {
      super(programRunId);
      error(t);
    }

    @Override
    protected void doSuspend() {
      // no-op
    }

    @Override
    protected void doResume() {
      // no-op
    }

    @Override
    protected void doStop() {
      // no-op
    }

    @Override
    protected void doCommand(String name, Object value) {
      // no-op
    }
  }
}
