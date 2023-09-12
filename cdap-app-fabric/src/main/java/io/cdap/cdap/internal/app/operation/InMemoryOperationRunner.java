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

package io.cdap.cdap.internal.app.operation;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.cdap.cdap.common.operation.LongRunningOperation;
import io.cdap.cdap.common.operation.LongRunningOperationRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operationrun.OperationError;
import io.cdap.cdap.proto.operationrun.OperationMeta;
import io.cdap.cdap.proto.operationrun.OperationType;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link OperationRunner} to run an operation in the same service.
 */
public class InMemoryOperationRunner<T> extends AbstractOperationRunner<T> implements
    AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryOperationRunner.class);

  private final OperationStatePublisher statePublisher;
  private final Lock lock;
  private final ExecutorService executor;
  private LongRunningOperation<T> currentOperation;

  /**
   * Default constructor.
   */
  @Inject
  public InMemoryOperationRunner(Map<OperationType, LongRunningOperation> operations,
      OperationStatePublisher statePublisher) {
    super(operations);
    this.statePublisher = statePublisher;
    this.lock = new ReentrantLock();
    this.executor = Executors.newSingleThreadExecutor(
        Threads.createDaemonThreadFactory("operation-runner-%d")
    );
  }

  // TODO(samik) It is currently rudimentary implementation to unblock the state management
  //  Handle states, publish failures and heartbeat messages
  @Override
  public void run(LongRunningOperationRequest<T> request)
      throws OperationTypeNotSupportedException, InvalidRequestTypeException {
    lock.lock();
    try {
      if (currentOperation != null) {
        throw new IllegalStateException("Operation already running");
      }
      currentOperation = getOperation(request);
      executor.execute(() -> {
        runInternal(request);
      });
      statePublisher.publishRunning();
    } finally {
      lock.unlock();
    }
  }

  private void runInternal(LongRunningOperationRequest<T> request) {
    try {
      ListenableFuture<OperationError> operationTask = currentOperation.run(request.getOperationRequest(), this::sendMetaUpdateMessage);
      Futures.addCallback(
          operationTask,
          new FutureCallback<OperationError>() {
            @Override
            public void onSuccess(@Nullable OperationError result) {
              if (result != null) {
                statePublisher.publishFailed(result);
              } else {
                statePublisher.publishSuccess();
              }
            }

            @Override
            public void onFailure(Throwable t) {
              statePublisher.publishFailed(getOperationErrorFromThrowable(request, t));
            }
          }
      );
    } catch (Exception e) {
      statePublisher.publishFailed(getOperationErrorFromThrowable(request, e));
    }
  }

  // TODO(samik): Implement graceful shutdown
  @Override
  public void stop(long timeout, TimeUnit timeoutUnit) throws Exception {
    close();
  }

  private void sendMetaUpdateMessage(OperationMeta meta) {
    statePublisher.publishMetaUpdate(meta);
  }

  private OperationError getOperationErrorFromThrowable(LongRunningOperationRequest<T> request,
      Throwable t) {
    OperationRunId runId = request.getRunId();
    LOG.error("Operation {} of namespace {} failed: {}", runId.getRun(), runId.getParent(),
        t.getMessage());
    return new OperationError(t.getMessage(), Collections.emptyList());
  }

  @Override
  public void close() throws Exception {
    lock.lock();
    try {
      if (currentOperation == null) {
        throw new IllegalStateException("No operation is running");
      }
      executor.shutdownNow();
      statePublisher.publishStopped();
    } finally {
      lock.unlock();
    }
  }
}
