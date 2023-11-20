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

import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that manages lifecycle of Operation.
 */
public class OperationLifecycleManager {

  private final TransactionRunner transactionRunner;
  private final OperationRuntime runtime;
  private final OperationStatePublisher statePublisher;

  private static final Logger LOG = LoggerFactory.getLogger(OperationLifecycleManager.class);


  @Inject
  OperationLifecycleManager(TransactionRunner transactionRunner, OperationRuntime runtime,
      OperationStatePublisher statePublisher) {
    this.transactionRunner = transactionRunner;
    this.runtime = runtime;
    this.statePublisher = statePublisher;
  }

  /**
   * Scan operations in a namespace.
   *
   * @param request scan request including filters and limit
   * @param txBatchSize batch size of transaction
   * @param consumer {@link Consumer} to process each scanned run
   * @return true if we have scanned till the request limit else return false. This will be used by
   *     the caller to identify if there is any further runs left to scan.
   */
  public boolean scanOperations(ScanOperationRunsRequest request, int txBatchSize,
      Consumer<OperationRunDetail> consumer) throws OperationRunNotFoundException, IOException {
    String lastKey = request.getScanAfter();
    int currentLimit = request.getLimit();

    while (currentLimit > 0) {
      ScanOperationRunsRequest batchRequest = ScanOperationRunsRequest
          .builder(request)
              .setScanAfter(lastKey)
              .setLimit(Math.min(txBatchSize, currentLimit))
              .build();

      request = batchRequest;

      lastKey = TransactionRunners.run(transactionRunner, context -> {
                return getOperationRunStore(context).scanOperations(batchRequest, consumer);
              }, IOException.class, OperationRunNotFoundException.class);

      if (lastKey == null) {
        break;
      }
      currentLimit -= txBatchSize;
    }
    return currentLimit == 0;
  }

  /**
   * Retrieves details of an operation run identified by the provided {@code OperationRunId}.
   *
   * @param runId The unique identifier for the operation run.
   * @return An {@code OperationRunDetail} object containing information about the specified
   *     operation run.
   * @throws OperationRunNotFoundException If the specified operation run is not found.
   */
  public OperationRunDetail getOperationRun(OperationRunId runId)
      throws IOException, OperationRunNotFoundException {
    return TransactionRunners.run(
        transactionRunner,
        context -> {
          return getOperationRunStore(context).getOperation(runId);
        },
        IOException.class,
        OperationRunNotFoundException.class);
  }

  /**
   * Validates state transition and sends STOPPING notification for an operation
   *
   * @param runId {@link OperationRunId} of the operation
   */
  public void sendStopNotification(OperationRunId runId)
      throws OperationRunNotFoundException, IOException, BadRequestException {
    // validate the run exists
    OperationRunDetail runDetail = getOperationRun(runId);
    if (!runDetail.getRun().getStatus().canTransitionTo(OperationRunStatus.STOPPING)) {
      throw new BadRequestException(String.format("Operation run in %s state cannot to be stopped",
          runDetail.getRun().getStatus()));
    }
    statePublisher.publishStopping(runId);
  }

  /**
   * Runs a given operation. It is the responsibility of the caller to validate state transition.
   *
   * @param detail {@link OperationRunDetail} of the operation
   */
  public OperationController startOperation(OperationRunDetail detail) {
    return runtime.run(detail);
  }


  /**
   * Initiate operation stop. It is the responsibility of the caller to validate state transition.
   *
   * @param detail {@link OperationRunDetail} of the operation
   * @throws IllegalStateException in case the operation is not running. It is the
   *     responsibility of the caller to handle the same
   */
  public void stopOperation(OperationRunDetail detail) {
    OperationController controller = runtime.getController(detail);
    if (controller == null) {
      throw new IllegalStateException("Operation is not running");
    }
    controller.stop();
  }

  /**
   * Checks if the operation is running. If not sends a failure notification
   * Called after service restart.
   *
   * @param detail {@link OperationRunDetail} of the operation
   */
  public void isRunning(OperationRunDetail detail, OperationStatePublisher statePublisher) {
    // If there is no active controller that means the operation is not running and should be failed
    if (runtime.getController(detail) == null) {
      LOG.debug("No running operation for {}, sending failure notification", detail.getRunId());
      statePublisher.publishFailed(detail.getRunId(),
          new OperationError("Failed after service restart as operation is not running",
              Collections.emptyList()));
      return;
    }
  }


  private OperationRunStore getOperationRunStore(StructuredTableContext context) {
    return new OperationRunStore(context);
  }
}
