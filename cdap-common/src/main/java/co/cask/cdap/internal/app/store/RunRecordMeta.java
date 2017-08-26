/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Store the meta information about program runs in CDAP.
 * This class contains all information the system needs about a run, which
 * includes information that should not be exposed to users. {@link RunRecord} contains fields that are exposed
 * to users, so everything else like the Twill runid should go here.
 */
public final class RunRecordMeta extends RunRecord {
  @SerializedName("twillrunid")
  private final String twillRunId;

  @SerializedName("systemargs")
  private final Map<String, String> systemArgs;

  @SerializedName("sourceid")
  @Nullable
  private final byte[] sourceId;

  public RunRecordMeta(String pid, long startTs, @Nullable Long runTs, @Nullable Long stopTs, ProgramRunStatus status,
                       @Nullable Map<String, String> properties, @Nullable Map<String, String> systemArgs,
                       @Nullable String twillRunId, byte[] sourceId) {
    super(pid, startTs, runTs, stopTs, status, properties);
    this.systemArgs = systemArgs;
    this.twillRunId = twillRunId;
    this.sourceId = sourceId;
  }

  public RunRecordMeta(RunRecordMeta started, @Nullable Long stopTs, ProgramRunStatus status, byte[] sourceId) {
    this(started.getPid(), started.getStartTs(), started.getRunTs(), stopTs, status, started.getProperties(),
         started.getSystemArgs(), started.getTwillRunId(), sourceId);
  }

  public RunRecordMeta(RunRecordMeta existing, Map<String, String> updatedProperties, byte[] sourceId) {
    this(existing.getPid(), existing.getStartTs(), existing.getRunTs(), existing.getStopTs(), existing.getStatus(),
         updatedProperties, existing.getSystemArgs(), existing.getTwillRunId(), sourceId);
  }

  @Nullable
  public String getTwillRunId() {
    return twillRunId;
  }

  @Nullable
  public Map<String, String> getSystemArgs() {
    return systemArgs;
  }

  @Nullable
  public byte[] getSourceId() {
    return sourceId;
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
    return Objects.equal(this.getPid(), that.getPid()) &&
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
    return Objects.hashCode(twillRunId, getPid(), getStartTs(), getRunTs(), getStopTs(), getStatus(), getProperties(),
                            Arrays.hashCode(getSourceId()));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("pid", getPid())
      .add("startTs", getStartTs())
      .add("runTs", getRunTs())
      .add("stopTs", getStopTs())
      .add("status", getStatus())
      .add("twillrunid", getTwillRunId())
      .add("properties", getProperties())
      .add("sourceId", getSourceId() == null ? null : Bytes.toHexString(getSourceId()))
      .toString();
  }
}
