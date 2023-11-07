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
import com.google.common.util.concurrent.SettableFuture;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
  private final Lock lock = new ReentrantLock();
  private final AtomicReference<OperationRunStatus> status = new AtomicReference<>();

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryOperationController.class);

  InMemoryOperationController(OperationRunId runId, OperationStatePublisher statePublisher,
      OperationDriver driver) {
    this.runId = runId;
    this.driver = driver;
    this.statePublisher = statePublisher;
    this.status.set(OperationRunStatus.PENDING);
    startListen(driver);
  }


  @Override
  public void stop() {
    //TODO(samik) verify state before calling stop
    lock.lock();
    try {
      this.status.set(OperationRunStatus.STOPPING);
      driver.stopAndWait();
      complete();
    } catch (Exception e) {
      LOG.warn("Exception when stopping operation {}", runId, e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ListenableFuture<OperationController> completionFuture() {
    return completionFuture;
  }

  private void startListen(Service service) {
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        status.set(OperationRunStatus.RUNNING);
        statePublisher.publishRunning(runId);
      }

      @Override
      public void terminated(Service.State from) {
        if (status.get().equals(OperationRunStatus.STOPPING)) {
          status.set(OperationRunStatus.KILLED);
        } else {
          status.set(OperationRunStatus.SUCCEEDED);
          statePublisher.publishSuccess(runId);
        }
        complete();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        status.set(OperationRunStatus.FAILED);
        if (failure instanceof OperationException) {
          statePublisher.publishFailed(runId, ((OperationException) failure).toOperationError());
        } else {
          statePublisher.publishFailed(runId, getOperationErrorFromThrowable(failure));
        }
        complete();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private void complete() {
    completionFuture.set(this);
  }

  private OperationError getOperationErrorFromThrowable(Throwable t) {
    LOG.debug("Operation {} of namespace {} failed", runId.getRun(), runId.getParent(), t);
    return new OperationError(t.getMessage(), Collections.emptyList());
  }
}
