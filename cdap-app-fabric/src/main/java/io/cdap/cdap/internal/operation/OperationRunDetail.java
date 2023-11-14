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

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationRun;
import java.util.Arrays;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Store the meta information about operation runs in CDAP. This class contains all information the
 * system needs about a run, which includes information that should not be exposed to users.
 * {@link io.cdap.cdap.proto.operation.OperationRun} contains fields that are exposed to users, so
 * everything else like the user request and principal goes here. // TODO(samik) Create a wrapper
 * class similar to OneOf Contains all possible operation requests only one of which should be
 * non-null. It is the responsibility of the runner to read the correct request class and pass it to
 * the operation.
 */
public class OperationRunDetail {

  // carries the OperationRunId, but we don't need to serialize it as it is already in the key of the store
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

  // Add any new operation request type here
  // Please also add the type in validateRequests
  @SerializedName("pullAppsRequest")
  @Nullable
  private final PullAppsRequest pullAppsRequest;

  protected OperationRunDetail(
      OperationRunId runId, OperationRun run,
      byte[] sourceId, @Nullable String principal,
      @Nullable PullAppsRequest pullAppsRequest) {
    this.runId = runId;
    this.run = run;
    this.sourceId = sourceId;
    this.principal = principal;
    this.pullAppsRequest = pullAppsRequest;
  }

  @Nullable
  public byte[] getSourceId() {
    return sourceId;
  }

  @Nullable
  public String getPrincipal() {
    return principal;
  }

  public PullAppsRequest getPullAppsRequest() {
    return pullAppsRequest;
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

    OperationRunDetail that = (OperationRunDetail) o;
    return Objects.equal(this.getRunId(), that.getRunId())
        && Objects.equal(this.getRun(), that.getRun())
        && Arrays.equals(this.getSourceId(), that.getSourceId())
        && Objects.equal(this.getPullAppsRequest(), that.getPullAppsRequest())
        && Objects.equal(this.getPrincipal(), that.getPrincipal());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(runId, run, Arrays.hashCode(sourceId), principal, pullAppsRequest);
  }

  /**
   * Builder to create a OperationRunDetail.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to create OperationRunDetail.
   *
   * @param detail existing detail to copy fields from.
   */
  public static Builder builder(OperationRunDetail detail) {
    return new Builder(detail);
  }

  /**
   * Builds RunRecordMetas.
   */
  public static class Builder {

    protected OperationRunId runId;
    protected OperationRun run;
    protected byte[] sourceId;
    protected String principal;
    protected PullAppsRequest pullAppsRequest;

    protected Builder() {
    }

    protected Builder(OperationRunDetail detail) {
      sourceId = detail.getSourceId();
      principal = detail.getPrincipal();
      run = detail.getRun();
      runId = detail.getRunId();
      pullAppsRequest = detail.getPullAppsRequest();
    }

    public Builder setSourceId(byte[] sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public Builder setPrincipal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder setRun(OperationRun run) {
      this.run = run;
      return this;
    }

    public Builder setRunId(OperationRunId runId) {
      this.runId = runId;
      return this;
    }


    public Builder setPullAppsRequest(PullAppsRequest pullAppsRequest) {
      this.pullAppsRequest = pullAppsRequest;
      return this;
    }

    /**
     * Validates input and returns a OperationRunDetail.
     */
    public OperationRunDetail build() {
      if (runId == null) {
        throw new IllegalArgumentException("run id must be specified.");
      }
      if (sourceId == null) {
        throw new IllegalArgumentException("Operation run source id must be specified.");
      }
      if (run == null) {
        throw new IllegalArgumentException("Operation run must be specified.");
      }
      // TODO(samik, CDAP-20809) Validate type to request mapping
      if (!validateRequests()) {
        throw new IllegalArgumentException("Exactly one request type can be non-null");
      }

      return new OperationRunDetail(runId, run, sourceId, principal, pullAppsRequest);
    }

    private boolean validateRequests() {
      // validate only one of the request is non-null
      return Stream.of(pullAppsRequest).filter(java.util.Objects::nonNull).count() == 1;
    }

  }
}
