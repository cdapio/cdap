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

import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResourceScopedError;
import java.util.Collection;

/**
 * Wrapper Exception class for operations. Provides capability to store per resource failure.
 */
public class OperationException extends Exception {

  private final Collection<OperationResourceScopedError> errors;

  public OperationException(String message, Collection<OperationResourceScopedError> errors) {
    super(message);
    this.errors = errors;
  }

  public OperationError toOperationError() {
    return new OperationError(getMessage(), errors);
  }
}
