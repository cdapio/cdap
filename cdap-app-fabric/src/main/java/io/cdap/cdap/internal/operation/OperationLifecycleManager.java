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

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Service that manages lifecycle of Operation. */
public class OperationLifecycleManager {

  private final TransactionRunner transactionRunner;
  private static final AtomicInteger sourceId = new AtomicInteger();
  private static final AtomicLong runIdTime = new AtomicLong(System.currentTimeMillis());
  private static final String testNamespace = "test";
  private static final PullAppsRequest input = new PullAppsRequest(Collections.emptySet(), null);

  @Inject
  OperationLifecycleManager(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
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
  public boolean scanOperations(
      ScanOperationRunsRequest request, int txBatchSize, Consumer<OperationRunDetail> consumer)
      throws OperationRunNotFoundException, IOException {
    String lastKey = request.getScanAfter();
    int currentLimit = request.getLimit();

    while (currentLimit > 0) {
      ScanOperationRunsRequest batchRequest =
          ScanOperationRunsRequest.builder(request)
              .setScanAfter(lastKey)
              .setLimit(Math.min(txBatchSize, currentLimit))
              .build();

      request = batchRequest;

      lastKey =
          TransactionRunners.run(
              transactionRunner,
              context -> {
                return getOperationRunStore(context).scanOperations(batchRequest, consumer);
              },
              IOException.class,
              OperationRunNotFoundException.class);

      if (lastKey == null) {
        break;
      }
      currentLimit -= txBatchSize;
    }
    return currentLimit == 0;
  }

  /**
   * Get a specific operation using run id in a namespace.
   *
   * @param runId run id of the operation to be fetched
   * @return operation run detail of the operation to be fetched. If not found, OperationRunNotFoundException is thrown.S
   */
  public OperationRunDetail getOperationRun(OperationRunId runId) throws IOException, OperationRunNotFoundException {
    OperationRunDetail operationRunDetail =
        TransactionRunners.run(
            transactionRunner,
            context -> {
              return getOperationRunStore(context).getOperation(runId);
            },
            IOException.class,
            OperationRunNotFoundException.class);

    return operationRunDetail;
  }
  
  private OperationRunStore getOperationRunStore(StructuredTableContext context) {
    return new OperationRunStore(context);
  }
}
