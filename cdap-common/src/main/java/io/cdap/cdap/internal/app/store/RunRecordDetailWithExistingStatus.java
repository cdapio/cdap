/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class is used to store {@link RunRecordDetail} with ProgramRunStatus of existing record with
 * same ProgramRunId.
 */
public final class RunRecordDetailWithExistingStatus extends RunRecordDetail {

  private final ProgramRunStatus existingStatus;

  private RunRecordDetailWithExistingStatus(ProgramRunId programRunId, long startTs,
      @Nullable Long runTs,
      @Nullable Long stopTs, @Nullable Long suspendTs, @Nullable Long resumeTs,
      @Nullable Long stoppingTs, @Nullable Long terminateTs, ProgramRunStatus status,
      @Nullable Map<String, String> properties, @Nullable Map<String, String> systemArgs,
      @Nullable String twillRunId, ProgramRunCluster cluster, ProfileId profileId,
      @Nullable String peerName, byte[] sourceId, @Nullable ArtifactId artifactId,
      @Nullable String principal, @Nullable ProgramRunStatus existingStatus,
      @Nullable String flowControlStatus) {
    super(programRunId, startTs, runTs, stopTs, suspendTs, resumeTs, stoppingTs, terminateTs,
        status, properties, systemArgs, twillRunId, cluster, profileId, peerName, sourceId,
        artifactId, principal, flowControlStatus);
    this.existingStatus = existingStatus;
  }

  @Nullable
  public ProgramRunStatus getExistingStatus() {
    return existingStatus;
  }

  /**
   * @param record existing record to copy fields from
   * @return Builder to create a RunRecordDetailWithExistingStatus, initialized with values from the
   *     specified existing record
   */
  public static Builder buildWithExistingStatus(RunRecordDetail record) {
    return new Builder(record);
  }

  public static class Builder extends RunRecordDetail.ABuilder<Builder> {

    private final ProgramRunStatus existingStatus;

    private Builder(RunRecordDetail record) {
      super(record);
      existingStatus = record.getStatus();
    }

    public RunRecordDetailWithExistingStatus build() {
      if (programRunId == null) {
        throw new IllegalArgumentException("Run record run id must be specified.");
      }
      if (sourceId == null) {
        throw new IllegalArgumentException("Run record source id must be specified.");
      }
      // we are not validating artifactId for null,
      // artifactId could be null for program starts that were recorded pre 5.0 but weren't processed
      // we don't want to throw exception while processing them
      return new RunRecordDetailWithExistingStatus(programRunId, startTs, runTs, stopTs, suspendTs,
          resumeTs, stoppingTs, terminateTs, status, properties, systemArgs,
          twillRunId, cluster, profileId, peerName, sourceId, artifactId,
          principal, existingStatus, flowControlStatus);
    }
  }
}
