/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

/**
 * Class to represent a timestamp in seconds and an int value.
 */
public final class TimeValue {

  private final long time;
  private final int value;

  public TimeValue(long time, int value) {
    this.time = time;
    this.value = value;
  }

  public long getTime() {
    return time;
  }

  public int getValue() {
    return value;
  }
}
