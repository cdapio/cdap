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

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.cdap.cdap.common.operation.LongRunningOperation;
import io.cdap.cdap.common.operation.LongRunningOperationRequest;
import io.cdap.cdap.proto.operationrun.OperationType;
import java.util.Map;

/**
 * Abstract runner implementation with common functionality like class loading.
 *
 * @param <T> The type of the operation request
 */
public abstract class AbstractOperationRunner<T> implements OperationRunner<T> {

  private final ImmutableMap<OperationType, LongRunningOperation> operations;

  protected AbstractOperationRunner(Map<OperationType, LongRunningOperation> operations) {
    this.operations = ImmutableMap.copyOf(operations);
  }


  /**
   * Converts an operation type to an operation class.
   *
   * @param request {@link LongRunningOperationRequest} for the operation
   * @return {@link LongRunningOperation} for the operation type in request
   * @throws OperationTypeNotSupportedException when the type is not mapped.
   */
  public LongRunningOperation<T> getOperation(LongRunningOperationRequest<T> request)
      throws OperationTypeNotSupportedException, InvalidRequestTypeException {
    LongRunningOperation operation = operations.get(request.getOperationType());
    if (operation == null) {
      throw new OperationTypeNotSupportedException(request.getOperationType());
    }
    if (request.getOperationRequest().getClass()
        .isAssignableFrom(operation.getRequestType().getClass())) {
      throw new InvalidRequestTypeException(request.getOperationType(),
          request.getOperationRequest().getClass().getName());
    }
    return (LongRunningOperation<T>) operation;
  }
}
