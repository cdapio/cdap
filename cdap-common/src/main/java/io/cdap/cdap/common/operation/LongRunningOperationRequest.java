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

package io.cdap.cdap.common.operation;

import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operationrun.OperationType;

/**
 * Encapsulates a operation run request.
 *
 * @param <T> The type of the operation request.
 */
public class LongRunningOperationRequest<T> {

  private final OperationRunId runId;
  private final OperationType operationType;
  private final T operationRequest;

  /**
   * Default constructor.
   */
  public LongRunningOperationRequest(OperationRunId runid, OperationType operationType, T operationRequest) {
    this.runId = runid;
    this.operationType = operationType;
    this.operationRequest = operationRequest;
  }

  public OperationRunId getRunId() {
    return runId;
  }

  public T getOperationRequest() {
    return operationRequest;
  }

  public OperationType getOperationType() {
    return operationType;
  }
}
