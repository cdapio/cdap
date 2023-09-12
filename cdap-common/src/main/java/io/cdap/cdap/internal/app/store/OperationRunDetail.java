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

package io.cdap.cdap.internal.app.store;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operationrun.OperationRun;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Store the meta information about operation runs in CDAP. This class contains all information the
 * system needs about a run, which includes information that should not be exposed to users.
 * {@link io.cdap.cdap.proto.operationrun.OperationRun} contains fields that are exposed to users,
 * so everything else like the user request and principal goes here
 *
 * @param <T> The type of the operation request
 */
public class OperationRunDetail<T> {

  // carries the OperationRunId, but we don't need to serialize it as it is already in the key of the  store
  private final transient OperationRunId runId;

  @SerializedName("run")
  private final OperationRun run;

  // sourceid refers to the tms message id which has updated this run details
  @SerializedName("sourceid")
  @Nullable
  private final byte[] sourceId;

  @SerializedName("principal")
  @Nullable
  private final String principal;

  @SerializedName("request")
  private final T request;

  protected OperationRunDetail(OperationRunId runId, OperationRun run, byte[] sourceId,
      @Nullable String principal,
      T request) {
    this.runId = runId;
    this.run = run;
    this.sourceId = sourceId;
    this.principal = principal;
    this.request = request;
  }

  @Nullable
  public byte[] getSourceId() {
    return sourceId;
  }

  @Nullable
  public String getPrincipal() {
    return principal;
  }

  public T getRequest() {
    return request;
  }

  public OperationRun getRun() {
    return run;
  }

  public OperationRunId getRunId() {
    return runId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OperationRunDetail<T> that = (OperationRunDetail<T>) o;
    return Objects.equal(this.getRun(), that.getRun())
        && Arrays.equals(this.getSourceId(), that.getSourceId())
        && Objects.equal(this.getRequest(), that.getRequest())
        && Objects.equal(this.getPrincipal(), that.getPrincipal());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), Arrays.hashCode(getSourceId()), getRequest(),
        getPrincipal());
  }

  /**
   * Builder to create a OperationRunDetail.
   */
  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  /**
   * Builder to create OperationRunDetail.
   *
   * @param detail existing detail to copy fields from.
   */
  public static <T> Builder<T> builder(OperationRunDetail<T> detail) {
    return new Builder<>(detail);
  }

  /**
   * Builds RunRecordMetas.
   */
  public static class Builder<T> {

    protected OperationRunId runId;
    protected OperationRun run;
    protected byte[] sourceId;
    protected String principal;
    protected T request;

    protected Builder() {
    }

    protected Builder(OperationRunDetail<T> detail) {
      sourceId = detail.getSourceId();
      principal = detail.getPrincipal();
      request = detail.getRequest();
      run = detail.getRun();
      runId = detail.getRunId();
    }

    public Builder<T> setSourceId(byte[] sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public Builder<T> setPrincipal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder<T> setRequest(T request) {
      this.request = request;
      return this;
    }

    public Builder<T> setRun(OperationRun run) {
      this.run = run;
      return this;
    }

    public Builder<T> setRun(OperationRunId runId) {
      this.runId = runId;
      return this;
    }

    /**
     * Validates input and returns a OperationRunDetail.
     */
    public OperationRunDetail<T> build() {
      if (runId == null) {
        throw new IllegalArgumentException("run id must be specified.");
      }
      if (request == null) {
        throw new IllegalArgumentException("Operation run request must be specified.");
      }
      if (sourceId == null) {
        throw new IllegalArgumentException("Operation run source id must be specified.");
      }
      if (run == null) {
        throw new IllegalArgumentException("Operation run must be specified.");
      }

      return new OperationRunDetail<>(runId, run, sourceId, principal, request);
    }
  }
}
