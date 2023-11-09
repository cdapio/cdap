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

package io.cdap.cdap.internal.operation;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.SettableFuture;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import java.util.Collections;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-Memory implementation of {@link OperationController}.
 */
public class InMemoryOperationController implements
    OperationController {

  private final OperationRunId runId;
  private final OperationStatePublisher statePublisher;
  private final OperationDriver driver;
  private final SettableFuture<OperationController> completionFuture = SettableFuture.create();
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryOperationController.class);

  InMemoryOperationController(OperationRunId runId, OperationStatePublisher statePublisher,
      OperationDriver driver) {
    this.runId = runId;
    this.driver = driver;
    this.statePublisher = statePublisher;
    startListen(driver);
  }


  @Override
  public ListenableFuture<OperationController> stop() {
    LOG.trace("Stopping operation {}", runId);
    driver.stop();
    return completionFuture;
  }

  @Override
  public ListenableFuture<OperationController> complete() {
    return completionFuture;
  }

  private void startListen(Service service) {
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        statePublisher.publishRunning(runId);
      }

      @Override
      public void terminated(Service.State from) {
        if (from.equals(State.STOPPING)) {
          statePublisher.publishKilled(runId);
        } else {
          statePublisher.publishSuccess(runId);
        }
        markComplete();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        if (failure instanceof OperationException) {
          statePublisher.publishFailed(runId, ((OperationException) failure).toOperationError());
        } else {
          statePublisher.publishFailed(runId, getOperationErrorFromThrowable(failure));
        }
        markComplete();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private void markComplete() {
    completionFuture.set(this);
  }

  private OperationError getOperationErrorFromThrowable(Throwable t) {
    LOG.debug("Operation {} of namespace {} failed", runId.getRun(), runId.getParent(), t);
    return new OperationError(t.getMessage(), Collections.emptyList());
  }
}
