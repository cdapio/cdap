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

package io.cdap.cdap.proto.operationrun;

import com.google.gson.annotations.SerializedName;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * This class records information for a particular operation run.
 */
public class OperationRun {

  @SerializedName("id")
  private final String id;

  @SerializedName("type")
  private final OperationType type;

  // derived from the operation status
  @SerializedName("done")
  private final boolean done;

  @SerializedName("status")
  private final OperationRunStatus status;

  @SerializedName("metadata")
  private final OperationMeta metadata;

  // operation run error if failed
  @Nullable
  @SerializedName("error")
  private final OperationError error;

  /**
   * Constructor for OperationRun.
   */
  protected OperationRun(String id, OperationType type, OperationRunStatus status, OperationMeta metadata,
      @Nullable OperationError error) {
    this.id = id;
    this.type = type;
    this.done = getStatus().isEndState();
    this.status = status;
    this.metadata = metadata;
    this.error = error;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OperationRun that = (OperationRun) o;

    return Objects.equals(this.id, that.id)
        && Objects.equals(this.type, that.type)
        && Objects.equals(this.status, that.status)
        && Objects.equals(this.metadata, that.metadata)
        && Objects.equals(this.error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, status, metadata, error);
  }

  /**
   * Creates a OperationRun Builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Create a OperationRun Builder from existing run.
   *
   * @param operationRun existing record to copy fields from
   */
  public static Builder builder(OperationRun operationRun) {
    return new Builder(operationRun);
  }

  public String getId() {
    return id;
  }

  public OperationRunStatus getStatus() {
    return status;
  }

  public OperationMeta getMetadata() {
    return metadata;
  }

  @Nullable
  public OperationError getError() {
    return error;
  }

  public OperationType getType() {
    return type;
  }

  /**
   * Builder to create OperationRun.
   *
   * @param <T> type of builder
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T extends Builder> {

    protected String runId;
    protected OperationType type;
    protected OperationRunStatus status;
    protected OperationMeta metadata;
    protected OperationError error;

    protected Builder() {
    }

    protected Builder(OperationRun other) {
      runId = other.getId();
      type = other.getType();
      status = other.getStatus();
      metadata = other.getMetadata();
      error = other.getError();
    }

    public T setStatus(OperationRunStatus status) {
      this.status = status;
      return (T) this;
    }

    public T setRunId(String runId) {
      this.runId = runId;
      return (T) this;
    }

    public T setMetadata(OperationMeta metadata) {
      this.metadata = metadata;
      return (T) this;
    }

    public T setError(OperationError error) {
      this.error = error;
      return (T) this;
    }

    public T setType(OperationType type) {
      this.type = type;
      return (T) this;
    }

    /**
     * Validates input and returns a OperationRun.
     */
    public OperationRun build() {
      if (runId == null) {
        throw new IllegalArgumentException("Operation run id must be specified.");
      }
      if (type == null) {
        throw new IllegalArgumentException("Operation run type must be specified.");
      }
      if (metadata == null) {
        throw new IllegalArgumentException("Operation run metadata must be specified.");
      }
      if (status == null) {
        throw new IllegalArgumentException("Operation run status must be specified.");
      }
      return new OperationRun(runId, type, status, metadata, error);
    }
  }
}
