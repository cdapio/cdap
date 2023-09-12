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

import io.cdap.cdap.proto.operationrun.OperationType;

/**
 * Thrown when the operation request in
 * {@link LongRunningOperationRequest} is not valid for requested
 * operation type.
 */
public class InvalidRequestTypeException extends Exception {

  public InvalidRequestTypeException(OperationType type, String requestClass) {
    super(String.format("Request class %s is not applicable operation type %s", requestClass,
        type.toString()));
  }
}
