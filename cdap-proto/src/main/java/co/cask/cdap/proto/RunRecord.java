/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.proto;

import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * This class records information for a particular run.
 */
public class RunRecord {
  @SerializedName("runid")
  private final String pid;

  @SerializedName("starting")
  @Nullable
  private final long startTs;

  @SerializedName("start")
  private final Long runTs;

  @Nullable
  @SerializedName("end")
  private final Long stopTs;

  @SerializedName("status")
  private final ProgramRunStatus status;

  @SerializedName("properties")
  private final Map<String, String> properties;

  public RunRecord(String pid, long startTs, @Nullable Long runTs, @Nullable Long stopTs, ProgramRunStatus status,
                   @Nullable Map<String, String> properties) {
    this.pid = pid;
    this.startTs = startTs;
    this.runTs = runTs;
    this.stopTs = stopTs;
    this.status = status;
    this.properties = properties == null ? Collections.<String, String>emptyMap() :
      Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }

  public RunRecord(RunRecord otherRunRecord) {
    this(otherRunRecord.getPid(), otherRunRecord.getStartTs(), otherRunRecord.getRunTs(), otherRunRecord.getStopTs(),
         otherRunRecord.getStatus(), otherRunRecord.getProperties());
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

  public ProgramRunStatus getStatus() {
    return status;
  }

  public Map<String, String> getProperties() {
    return properties;
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

    return Objects.equals(this.pid, that.pid) &&
      Objects.equals(this.startTs, that.startTs) &&
      Objects.equals(this.runTs, that.runTs) &&
      Objects.equals(this.stopTs, that.stopTs) &&
      Objects.equals(this.status, that.status) &&
      Objects.equals(this.properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pid, startTs, runTs, stopTs, status, properties);
  }

  @Override
  public String toString() {
    return "RunRecord{" +
      "pid='" + pid + '\'' +
      ", startTs=" + startTs +
      ", runTs=" + runTs +
      ", stopTs=" + stopTs +
      ", status=" + status +
      ", properties=" + properties +
      '}';
  }
}
