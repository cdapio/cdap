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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.cdap.cdap.common.operation.LongRunningOperation;
import io.cdap.cdap.common.operation.LongRunningOperationRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operationrun.OperationError;
import io.cdap.cdap.proto.operationrun.OperationType;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link OperationRunner} to run an operation in the same service.
 */
public class InMemoryOperationRunner<T> extends AbstractOperationRunner<T> {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryOperationRunner.class);

  private final OperationStatePublisher statePublisher;
  private final Lock lock;
  private OperationDriver<T> driver;

  /**
   * Default constructor.
   *
   * @param operations A map of {@link OperationType} to {@link LongRunningOperation} injected
   *     via {@link com.google.inject.multibindings.MapBinder}
   * @param statePublisher Publishes the current operation state.
   */
  @Inject
  public InMemoryOperationRunner(Map<OperationType, LongRunningOperation> operations,
      OperationStatePublisher statePublisher) {
    super(operations);
    this.statePublisher = statePublisher;
    this.lock = new ReentrantLock();
  }

  // TODO(samik) It is currently rudimentary implementation to unblock the state management
  //  Handle states, publish failures and heartbeat messages
  @Override
  public void run(LongRunningOperationRequest<T> request)
      throws OperationTypeNotSupportedException, InvalidRequestTypeException {
    lock.lock();
    try {
      if (driver != null) {
        throw new IllegalStateException("Operation already running");
      }
      driver = new OperationDriver<>(getOperation(request), request, statePublisher);
      driver.start();
      statePublisher.publishRunning();
    } finally {
      lock.unlock();
    }
  }

  // TODO(samik): Implement graceful shutdown
  @Override
  public void stop(long timeout, TimeUnit timeoutUnit) throws Exception {
    lock.lock();
    try {
      if (driver == null) {
        throw new IllegalStateException("No operation is running");
      }
      driver.stopAndWait();
      statePublisher.publishStopped();
    } finally {
      lock.unlock();
    }
  }

  private static final class OperationDriver<T> extends AbstractExecutionThreadService {

    private final LongRunningOperation<T> operation;
    private final LongRunningOperationRequest<T> request;

    private final OperationStatePublisher statePublisher;

    private ExecutorService executor;

    private OperationDriver(LongRunningOperation<T> operation,
        LongRunningOperationRequest<T> request,
        OperationStatePublisher statePublisher) {
      this.operation = operation;
      this.request = request;
      this.statePublisher = statePublisher;
    }

    @Override
    protected void run() throws Exception {
      // TODO(samik) see if we can use Futures.addCallback or addListener
      try {
        ListenableFuture<OperationError> error = operation.run(
            request.getOperationRequest(),
            statePublisher::publishMetaUpdate);
        if (error.get() != null) {
          statePublisher.publishFailed(error.get());
        } else {
          statePublisher.publishSuccess();
        }
      } catch (InterruptedException e) {
        // TODO(samik) handle InterruptedException cleanly
      } catch (Exception e) {
        statePublisher.publishFailed(getOperationErrorFromThrowable(request, e));
      }
    }

    @Override
    protected void triggerShutdown() {
      if (executor != null) {
        executor.shutdownNow();
      }
    }

    @Override
    protected Executor executor() {
      executor = Executors.newSingleThreadExecutor(
          Threads.createDaemonThreadFactory("operation-runner-%d")
      );
      return executor;
    }

    private OperationError getOperationErrorFromThrowable(LongRunningOperationRequest<T> request,
        Throwable t) {
      OperationRunId runId = request.getRunId();
      LOG.error("Operation {} of namespace {} failed: {}", runId.getRun(), runId.getParent(),
          t.getMessage());
      return new OperationError(t.getMessage(), Collections.emptyList());
    }
  }
}
