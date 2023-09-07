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

package io.cdap.cdap.api.service.operation;

import java.util.List;

public class OperationRun {
  private final String operationId;
  private final String namespace;
  private final String operationType;
  private OperationStatus status;
  private final Long createdAt;
  private Long updatedAt;
  private OperationMeta metadata;
  private List<OperationError> errors;

  public OperationRun(String operationId, String namespace, String operationType, Long createdAt) {
    this.operationId = operationId;
    this.operationType = operationType;
    this.namespace = namespace;
    this.createdAt = createdAt;
    this.updatedAt = createdAt;
    this.status = OperationStatus.PENDING;
  }

  public OperationRun(
      String operationId, String namespace, String operationType, OperationStatus status,
      Long createdAt, Long updatedAt, OperationMeta metadata, List<OperationError> errors
  ) {
    this.operationId = operationId;
    this.operationType = operationType;
    this.namespace = namespace;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.status = status;
    this.metadata = metadata;
    this.errors = errors;
  }

  public String getOperationId() {
    return operationId;
  }

  public String getOperationType() {
    return operationType;
  }

  public String getNamespace() {
    return namespace;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public Long getUpdatedAt() {
    return updatedAt;
  }

  public OperationStatus getStatus() {
    return status;
  }

  public OperationMeta getMetadata() {
    return metadata;
  }

  public List<OperationError> getErrors() {
    return errors;
  }

  public void setUpdatedAt(Long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public void setStatus(OperationStatus status) {
    this.status = status;
  }

  public void setMetadata(OperationMeta metadata) {
    this.metadata = metadata;
  }

  public void setErrors(List<OperationError> errors) {
    this.errors = errors;
  }
}
