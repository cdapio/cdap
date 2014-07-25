/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.app.program;

import javax.annotation.Nullable;

/**
 * This class record information for a particular run.
 */
public final class RunRecord {
  private final String pid;
  private final long startTs;
  private final long stopTs;
  private final String endStatus;

  public RunRecord(String pid, long startTs) {
    this.pid = pid;
    this.startTs = startTs;
    this.stopTs = -1;
    this.endStatus = null;
  }

  public RunRecord(String pid, long startTs, long stopTs, String endStatus) {
    this.pid = pid;
    this.startTs = startTs;
    this.stopTs = stopTs;
    this.endStatus = endStatus;
  }

  public RunRecord(RunRecord started, long stopTs, String endStatus) {
    this.pid = started.pid;
    this.startTs = started.startTs;
    this.stopTs = stopTs;
    this.endStatus = endStatus;
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
