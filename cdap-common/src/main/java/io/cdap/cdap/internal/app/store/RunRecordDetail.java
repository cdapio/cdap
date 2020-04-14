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
package io.cdap.cdap.internal.app.store;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Store the meta information about program runs in CDAP.
 * This class contains all information the system needs about a run, which
 * includes information that should not be exposed to users. {@link RunRecord} contains fields that are exposed
 * to users, so everything else like the Twill runid should go here.
 */
public final class RunRecordDetail extends RunRecord {

  // carries the ProgramRunId, but we don't need to serialize it as it is already in the key of the meta data store
  private final transient ProgramRunId programRunId;

  @SerializedName("twillrunid")
  private final String twillRunId;

  @SerializedName("systemargs")
  private final Map<String, String> systemArgs;

  @SerializedName("sourceid")
  @Nullable
  private final byte[] sourceId;

  @SerializedName("artifactId")
  @Nullable
  private final ArtifactId artifactId;

  @SerializedName("principal")
  @Nullable
  private final String principal;

  // carries the user arguments decoded from properties. No need to serialize since it is from the properties.
  private transient volatile Map<String, String> userArgs;

  private RunRecordDetail(ProgramRunId programRunId, long startTs, @Nullable Long runTs, @Nullable Long stopTs,
                          @Nullable Long suspendTs, @Nullable Long resumeTs,
                          ProgramRunStatus status, @Nullable Map<String, String> properties,
                          @Nullable Map<String, String> systemArgs, @Nullable String twillRunId,
                          ProgramRunCluster cluster, ProfileId profileId, byte[] sourceId,
                          @Nullable ArtifactId artifactId, @Nullable String principal) {
    super(programRunId.getRun(), startTs, runTs, stopTs, suspendTs, resumeTs, status, properties, cluster, profileId);
    this.programRunId = programRunId;
    this.systemArgs = systemArgs;
    this.twillRunId = twillRunId;
    this.sourceId = sourceId;
    this.artifactId = artifactId;
    this.principal = principal;
  }

  @Nullable
  public String getTwillRunId() {
    return twillRunId;
  }

  public Map<String, String> getSystemArgs() {
    return systemArgs == null ? Collections.emptyMap() : systemArgs;
  }

  public Map<String, String> getUserArgs() {
    Map<String, String> userArgs = this.userArgs;
    if (userArgs != null) {
      return userArgs;
    }
    Map<String, String> properties = getProperties();
    if (properties != null) {
      String runtimeArgs = properties.get("runtimeArgs");
      if (runtimeArgs != null) {
        userArgs = new LinkedHashMap<>(new Gson().fromJson(runtimeArgs,
                                                           new TypeToken<Map<String, String>>() { }.getType()));
      }
    } else {
      userArgs = Collections.emptyMap();
    }
    this.userArgs = userArgs;
    return userArgs;
  }

  @Nullable
  public byte[] getSourceId() {
    return sourceId;
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  /**
   * returns the artifact id of the application the program belongs to.
   * @return artifact id, null only for older run records
   */
  @Nullable
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  @Nullable
  public String getPrincipal() {
    return principal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RunRecordDetail that = (RunRecordDetail) o;
    return Objects.equal(this.getProgramRunId(), that.getProgramRunId()) &&
      Objects.equal(this.getStartTs(), that.getStartTs()) &&
      Objects.equal(this.getRunTs(), that.getRunTs()) &&
      Objects.equal(this.getStopTs(), that.getStopTs()) &&
      Objects.equal(this.getSuspendTs(), that.getSuspendTs()) &&
      Objects.equal(this.getResumeTs(), that.getResumeTs()) &&
      Objects.equal(this.getStatus(), that.getStatus()) &&
      Objects.equal(this.getProperties(), that.getProperties()) &&
      Objects.equal(this.getTwillRunId(), that.getTwillRunId()) &&
      Arrays.equals(this.getSourceId(), that.getSourceId()) &&
      Objects.equal(this.getArtifactId(), that.getArtifactId()) &&
      Objects.equal(this.principal, that.principal);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getProgramRunId(), getStartTs(), getRunTs(), getStopTs(), getSuspendTs(), getResumeTs(),
                            getStatus(), getProperties(), getTwillRunId(), Arrays.hashCode(getSourceId()),
                            getArtifactId(), getPrincipal());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("programRunId", getProgramRunId())
      .add("startTs", getStartTs())
      .add("runTs", getRunTs())
      .add("stopTs", getStopTs())
      .add("suspendTs", getSuspendTs())
      .add("resumeTs", getResumeTs())
      .add("status", getStatus())
      .add("twillrunid", getTwillRunId())
      .add("systemArgs", getSystemArgs())
      .add("properties", getProperties())
      .add("cluster", getCluster())
      .add("profile", getProfileId())
      .add("sourceId", getSourceId() == null ? null : Bytes.toHexString(getSourceId()))
      .add("artifactId", getArtifactId())
      .add("principal", getPrincipal())
      .toString();
  }

  /**
   * @return Builder to create a RunRecordDetail
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @param record existing record to copy fields from
   * @return Builder to create a RunRecordDetail, initialized with values from the specified existing record
   */
  public static Builder builder(RunRecordDetail record) {
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
    private String principal;
    private ArtifactId artifactId;

    private Builder() {
      systemArgs = new HashMap<>();
    }

    private Builder(RunRecordDetail record) {
      super(record);
      programRunId = record.getProgramRunId();
      twillRunId = record.getTwillRunId();
      systemArgs = new HashMap<>(record.getSystemArgs());
      sourceId = record.getSourceId();
      principal = record.getPrincipal();
      artifactId = record.getArtifactId();
    }

    public Builder setProgramRunId(ProgramRunId programRunId) {
      this.programRunId = programRunId;
      return this;
    }

    public Builder setTwillRunId(@Nullable String twillRunId) {
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

    public Builder setPrincipal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder setArtifactId(ArtifactId artifactId) {
      this.artifactId = artifactId;
      return this;
    }

    public RunRecordDetail build() {
      if (programRunId == null) {
        throw new IllegalArgumentException("Run record run id must be specified.");
      }
      if (sourceId == null) {
        throw new IllegalArgumentException("Run record source id must be specified.");
      }
      // we are not validating artifactId for null,
      // artifactId could be null for program starts that were recorded pre 5.0 but weren't processed
      // we don't want to throw exception while processing them
      return new RunRecordDetail(programRunId, startTs, runTs, stopTs, suspendTs, resumeTs,
                                 status, properties, systemArgs, twillRunId, cluster, profileId,
                                 sourceId, artifactId, principal);
    }
  }
}
