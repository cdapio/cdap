/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

/**
 * Interpolate a value between two other time values.
 */
public interface Interpolator {

  /**
   * Given start and end TimeValues, and a time in-between the two, return a TimeValue for the in-between time.
   */
  public int interpolate(TimeValue start, TimeValue end, long ts);

  /**
   * Data points that are more than this many seconds apart will not cause interpolation to occur and will instead
   * return 0 for any point in between.
   */
  public long getMaxAllowedGap();
}
