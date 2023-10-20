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

import io.cdap.cdap.proto.operation.OperationRunStatus;
import javax.annotation.Nullable;

/**
 * This class defined various filters that can be applied during operation runs scanning.
 */
public class OperationRunFilter {

  @Nullable
  private final String operationType;
  @Nullable
  private final OperationRunStatus status;
  // TODO(samik) status and type filters as list

  public OperationRunFilter(@Nullable String operationType, @Nullable OperationRunStatus status) {
    this.operationType = operationType;
    this.status = status;
  }

  @Nullable
  public String getOperationType() {
    return operationType;
  }

  @Nullable
  public OperationRunStatus getStatus() {
    return status;
  }

  public static OperationRunFilter emptyFilter() {
    return new OperationRunFilter(null, null);
  }
}
