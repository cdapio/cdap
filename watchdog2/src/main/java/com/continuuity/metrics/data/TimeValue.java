/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

/**
 * Class to represent a timestamp in seconds and an int value.
 */
public final class TimeValue {

  private final long timestamp;
  private final int value;

  public TimeValue(long timestamp, int value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getValue() {
    return value;
  }
}
