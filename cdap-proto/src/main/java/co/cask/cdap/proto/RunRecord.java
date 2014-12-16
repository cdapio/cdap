/*
 * Copyright © 2014 Cask Data, Inc.
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

import javax.annotation.Nullable;

/**
 * This class records information for a particular run.
 */
public final class RunRecord {
  @SerializedName("runid")
  private final String pid;

  @SerializedName("start")
  private final long startTs;

  @SerializedName("end")
  private final Long stopTs;

  @SerializedName("status")
  private final ProgramRunStatus status;

  public RunRecord(String pid, long startTs, Long stopTs, ProgramRunStatus status) {
    this.pid = pid;
    this.startTs = startTs;
    this.stopTs = stopTs;
    this.status = status;
  }

  public RunRecord(RunRecord started, long stopTs, ProgramRunStatus status) {
    this(started.pid, started.startTs, stopTs, status);
  }

  public String getPid() {
    return pid;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getStopTs() {
    return stopTs;
  }

  public ProgramRunStatus getStatus() {
    return status;
  }
}
