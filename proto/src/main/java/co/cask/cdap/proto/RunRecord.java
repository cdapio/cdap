/*
 * Copyright 2014 Cask, Inc.
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
  private final long stopTs;

  @SerializedName("status")
  private final String endStatus;

  public RunRecord(String pid, long startTs) {
    this(pid, startTs, -1, null);
  }

  public RunRecord(String pid, long startTs, long stopTs, String endStatus) {
    this.pid = pid;
    this.startTs = startTs;
    this.stopTs = stopTs;
    this.endStatus = endStatus;
  }

  public RunRecord(RunRecord started, long stopTs, String endStatus) {
    this(started.pid, started.startTs, stopTs, endStatus);
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

  @Nullable
  public String getEndStatus() {
    return endStatus;
  }
}
