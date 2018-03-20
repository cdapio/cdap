/*
 * Copyright © 2015-2018 Cask Data, Inc.
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
package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.proto.ProgramRunCluster;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Store the meta information about program runs in CDAP.
 * This class contains all information the system needs about a run, which
 * includes information that should not be exposed to users. {@link RunRecord} contains fields that are exposed
 * to users, so everything else like the Twill runid should go here.
 */
public final class RunRecordMeta extends RunRecord {

  // carries the ProgramRunId, but we don't need to serialize it as it is already in the key of the meta data store
  private final transient ProgramRunId programRunId;

  @SerializedName("twillrunid")
  private final String twillRunId;

  @SerializedName("systemargs")
  private final Map<String, String> systemArgs;

  @SerializedName("sourceid")
  @Nullable
  private final byte[] sourceId;

  private RunRecordMeta(ProgramRunId programRunId, long startTs, @Nullable Long runTs, @Nullable Long stopTs,
                        ProgramRunStatus status, @Nullable Map<String, String> properties,
                        @Nullable Map<String, String> systemArgs, @Nullable String twillRunId,
                        ProgramRunCluster cluster, byte[] sourceId) {
    super(programRunId.getRun(), startTs, runTs, stopTs, status, properties, cluster);
    this.programRunId = programRunId;
    this.systemArgs = systemArgs;
    this.twillRunId = twillRunId;
    this.sourceId = sourceId;
  }

  @Nullable
  public String getTwillRunId() {
    return twillRunId;
  }

  public Map<String, String> getSystemArgs() {
    return systemArgs == null ? Collections.emptyMap() : systemArgs;
  }

  @Nullable
  public byte[] getSourceId() {
    return sourceId;
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RunRecordMeta that = (RunRecordMeta) o;
    return Objects.equal(this.getProgramRunId(), that.getProgramRunId()) &&
      Objects.equal(this.getStartTs(), that.getStartTs()) &&
      Objects.equal(this.getRunTs(), that.getRunTs()) &&
      Objects.equal(this.getStopTs(), that.getStopTs()) &&
      Objects.equal(this.getStatus(), that.getStatus()) &&
      Objects.equal(this.getProperties(), that.getProperties()) &&
      Objects.equal(this.getTwillRunId(), that.getTwillRunId()) &&
      Arrays.equals(this.getSourceId(), that.getSourceId());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getProgramRunId(), getStartTs(), getRunTs(), getStopTs(),
                            getStatus(), getProperties(), getTwillRunId(), Arrays.hashCode(getSourceId()));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("programRunId", getProgramRunId())
      .add("startTs", getStartTs())
      .add("runTs", getRunTs())
      .add("stopTs", getStopTs())
      .add("status", getStatus())
      .add("twillrunid", getTwillRunId())
      .add("properties", getProperties())
      .add("sourceId", getSourceId() == null ? null : Bytes.toHexString(getSourceId()))
      .toString();
  }

  /**
   * @return Builder to create a RunRecordMeta
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @param record existing record to copy fields from
   * @return Builder to create a RunRecordMeta, initialized with values from the specified existing record
   */
  public static Builder builder(RunRecordMeta record) {
    return new Builder(record);
  }

  /**
   * Builds RunRecordMetas.
   */
  public static class Builder extends RunRecord.Builder<Builder> {
    private ProgramRunId programRunId;
    private String twillRunId;
    private Map<String, String> systemArgs;
    private byte[] sourceId;

    private Builder() {
      systemArgs = new HashMap<>();
    }

    private Builder(RunRecordMeta record) {
      super(record);
      programRunId = record.getProgramRunId();
      twillRunId = record.getTwillRunId();
      systemArgs = new HashMap<>(record.getSystemArgs());
      sourceId = record.getSourceId();
    }

    public Builder setProgramRunId(ProgramRunId programRunId) {
      this.programRunId = programRunId;
      return this;
    }

    public Builder setTwillRunId(String twillRunId) {
      this.twillRunId = twillRunId;
      return this;
    }

    public Builder setSystemArgs(@Nullable Map<String, String> systemArgs) {
      this.systemArgs.clear();
      if (systemArgs != null) {
        this.systemArgs.putAll(systemArgs);
      }
      return this;
    }

    public Builder setSourceId(byte[] sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public RunRecordMeta build() {
      if (programRunId == null) {
        throw new IllegalArgumentException("Run record run id must be specified.");
      }
      if (sourceId == null) {
        throw new IllegalArgumentException("Run record source id must be specified.");
      }
      return new RunRecordMeta(programRunId, startTs, runTs, stopTs, status, properties, systemArgs, twillRunId,
                               cluster, sourceId);
    }
  }
}
