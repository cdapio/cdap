/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.proto.id.ProfileId;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * This class records information for a particular run. The timestamp in the run record has time
 * unit SECONDS
 */
public class RunRecord {

  @SerializedName("runid")
  private final String pid;

  @SerializedName("starting")
  private final long startTs;

  @Nullable
  @SerializedName("start")
  private final Long runTs;

  @Nullable
  @SerializedName("end")
  private final Long stopTs;

  @Nullable
  @SerializedName("suspend")
  private final Long suspendTs;

  @Nullable
  @SerializedName("resume")
  private final Long resumeTs;

  @Nullable
  @SerializedName("stopping")
  private final Long stoppingTs;

  // the timestamp at which the program
  // must be stopped, ideally in the future
  @Nullable
  @SerializedName("terminate")
  private final Long terminateTs;

  @SerializedName("status")
  private final ProgramRunStatus status;

  @SerializedName("properties")
  private final Map<String, String> properties;

  @SerializedName("cluster")
  private final ProgramRunCluster cluster;

  @SerializedName("profile")
  private final ProfileId profileId;

  // the name of the tethered peer that the program was initiated by, if any
  @SerializedName("peerName")
  private final String peerName;

  // the version of the pipeline the run associated with
  @SerializedName("version")
  private final String version;

  /**
   * @deprecated use {@link #builder()} instead.
   */
  @Deprecated
  public RunRecord(String pid, long startTs, @Nullable Long runTs, @Nullable Long stopTs,
      @Nullable Long suspendTs,
      @Nullable Long resumeTs, @Nullable Long stoppingTs, @Nullable Long terminateTs,
      ProgramRunStatus status, @Nullable Map<String, String> properties, ProgramRunCluster cluster,
      ProfileId profileId, @Nullable String peerName, @Nullable String version) {
    this.pid = pid;
    this.startTs = startTs;
    this.runTs = runTs;
    this.stopTs = stopTs;
    this.suspendTs = suspendTs;
    this.resumeTs = resumeTs;
    this.stoppingTs = stoppingTs;
    this.terminateTs = terminateTs;
    this.status = status;
    this.properties = properties == null ? Collections.emptyMap() :
        Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    this.cluster = cluster;
    this.profileId = profileId;
    this.peerName = peerName;
    this.version = version;
  }

  /**
   * @deprecated use {@link #builder(RunRecord)} instead.
   */
  public RunRecord(RunRecord otherRunRecord) {
    this(otherRunRecord.getPid(), otherRunRecord.getStartTs(), otherRunRecord.getRunTs(),
        otherRunRecord.getStopTs(),
        otherRunRecord.getSuspendTs(), otherRunRecord.getResumeTs(), otherRunRecord.getStoppingTs(),
        otherRunRecord.getTerminateTs(), otherRunRecord.getStatus(), otherRunRecord.getProperties(),
        otherRunRecord.getCluster(), otherRunRecord.getProfileId(), otherRunRecord.getPeerName(),
        otherRunRecord.getVersion());
  }

  public String getPid() {
    return pid;
  }

  public long getStartTs() {
    return startTs;
  }

  @Nullable
  public Long getRunTs() {
    return runTs;
  }

  @Nullable
  public Long getStopTs() {
    return stopTs;
  }

  @Nullable
  public Long getSuspendTs() {
    return suspendTs;
  }

  @Nullable
  public Long getResumeTs() {
    return resumeTs;
  }

  @Nullable
  public Long getStoppingTs() {
    return stoppingTs;
  }

  @Nullable
  public Long getTerminateTs() {
    return terminateTs;
  }

  public ProgramRunStatus getStatus() {
    return status;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public ProgramRunCluster getCluster() {
    // null check for backwards compat for run records that did not have any cluster
    return cluster == null ? new ProgramRunCluster(ProgramRunClusterStatus.DEPROVISIONED, null,
        null) : cluster;
  }

  public ProfileId getProfileId() {
    // for backward compatibility, return native profile if the profile id is null
    return profileId == null ? ProfileId.NATIVE : profileId;
  }

  @Nullable
  public String getPeerName() {
    return peerName;
  }

  @Nullable
  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RunRecord that = (RunRecord) o;

    return Objects.equals(this.pid, that.pid)
        && Objects.equals(this.startTs, that.startTs)
        && Objects.equals(this.runTs, that.runTs)
        && Objects.equals(this.stopTs, that.stopTs)
        && Objects.equals(this.suspendTs, that.suspendTs)
        && Objects.equals(this.resumeTs, that.resumeTs)
        && Objects.equals(this.stoppingTs, that.stoppingTs)
        && Objects.equals(this.terminateTs, that.terminateTs)
        && Objects.equals(this.status, that.status)
        && Objects.equals(this.properties, that.properties)
        && Objects.equals(this.cluster, that.cluster)
        && Objects.equals(this.profileId, that.profileId)
        && Objects.equals(this.peerName, that.peerName)
        && Objects.equals(this.version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pid, startTs, runTs, stopTs, suspendTs, resumeTs, stoppingTs, terminateTs,
        status,
        properties, cluster, profileId, peerName, version);
  }

  @Override
  public String toString() {
    return "RunRecord{"
        + "pid='" + pid + '\''
        + ", startTs=" + startTs
        + ", runTs=" + runTs
        + ", stopTs=" + stopTs
        + ", suspendTs=" + suspendTs
        + ", resumeTs=" + resumeTs
        + ", stoppingTs=" + stoppingTs
        + ", terminateTs=" + terminateTs
        + ", status=" + status
        + ", properties=" + properties
        + ", cluster=" + cluster
        + ", profile=" + profileId
        + ", peerName=" + peerName
        + ", version=" + version
        + '}';
  }

  /**
   * @return Builder to create a RunRecord
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @param runRecord existing record to copy fields from
   * @return Builder to create a RunRecord, initialized with values from the specified existing
   *     record
   */
  public static Builder builder(RunRecord runRecord) {
    return new Builder(runRecord);
  }

  /**
   * Builder to create RunRecords.
   *
   * @param <T> type of builder
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T extends Builder> {

    protected ProgramRunStatus status;
    protected String pid;
    protected Long startTs;
    protected Long runTs;
    protected Long stopTs;
    protected Long suspendTs;
    protected Long resumeTs;
    protected Long stoppingTs;
    protected Long terminateTs;
    protected Map<String, String> properties;
    protected ProgramRunCluster cluster;
    protected ProfileId profileId;
    protected String peerName;
    protected String version;

    protected Builder() {
      properties = new HashMap<>();
    }

    protected Builder(RunRecord other) {
      status = other.getStatus();
      pid = other.getPid();
      startTs = other.getStartTs();
      runTs = other.getRunTs();
      suspendTs = other.getSuspendTs();
      resumeTs = other.getResumeTs();
      stoppingTs = other.getStoppingTs();
      terminateTs = other.getTerminateTs();
      stopTs = other.getStopTs();
      properties = new HashMap<>(other.getProperties());
      cluster = other.getCluster();
      profileId = other.getProfileId();
      peerName = other.getPeerName();
      version = other.getVersion();
    }

    public T setStatus(ProgramRunStatus status) {
      this.status = status;
      return (T) this;
    }

    public T setRunId(String runId) {
      this.pid = runId;
      return (T) this;
    }

    public T setStartTime(long startTs) {
      this.startTs = startTs;
      return (T) this;
    }

    public T setRunTime(Long runTs) {
      this.runTs = runTs;
      return (T) this;
    }

    public T setStopTime(Long stopTs) {
      this.stopTs = stopTs;
      return (T) this;
    }

    public T setSuspendTime(Long suspendTime) {
      this.suspendTs = suspendTime;
      return (T) this;
    }

    public T setResumeTime(Long resumeTs) {
      this.resumeTs = resumeTs;
      return (T) this;
    }

    public T setStoppingTime(Long stoppingTs) {
      this.stoppingTs = stoppingTs;
      return (T) this;
    }

    public T setTerminateTs(Long terminateTs) {
      this.terminateTs = terminateTs;
      return (T) this;
    }

    public T setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return (T) this;
    }

    public T setCluster(ProgramRunCluster cluster) {
      this.cluster = cluster;
      return (T) this;
    }

    public T setProfileId(ProfileId profileId) {
      this.profileId = profileId;
      return (T) this;
    }

    public T setPeerName(String peerName) {
      this.peerName = peerName;
      return (T) this;
    }

    public T setVersion(String version) {
      this.version = version;
      return (T) this;
    }

    public RunRecord build() {
      if (pid == null) {
        throw new IllegalArgumentException("Run record run id must be specified.");
      }
      if (startTs == null) {
        throw new IllegalArgumentException("Run record start time must be specified.");
      }
      if (cluster == null) {
        throw new IllegalArgumentException("Run record cluster must be specified.");
      }
      if (status == null) {
        throw new IllegalArgumentException("Run record status must be specified.");
      }
      return new RunRecord(pid, startTs, runTs, stopTs, suspendTs, resumeTs, stoppingTs,
          terminateTs,
          status, properties, cluster, profileId, peerName, version);
    }
  }
}
