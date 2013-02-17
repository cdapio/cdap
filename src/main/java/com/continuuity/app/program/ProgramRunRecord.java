/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.app.program;

public class ProgramRunRecord {
  private String pid;
  private long startTs;
  private long stopTs;
  private Status endStatus;

  public ProgramRunRecord(final String pid, final long startTs, final long stopTs, final Status endStatus) {
    this.pid = pid;
    this.startTs = startTs;
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

  public Status getEndStatus() {
    return endStatus;
  }
}
