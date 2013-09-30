/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class InterpolatorTest {

  @Test
  public void testStepInterpolator() {
    Interpolator interpolator = new Interpolators.Step();
    long startTs = 1;
    long endTs = 10;
    TimeValue start = new TimeValue(startTs, 5);
    TimeValue end = new TimeValue(endTs, 3);
    for (long i = startTs; i < endTs; i++) {
      Assert.assertEquals(5, interpolator.interpolate(start, end, i));
    }
    Assert.assertEquals(3, interpolator.interpolate(start, end, endTs));
  }

  @Test
  public void testSmallSlopeLinearInterpolator() {
    Interpolator interpolator = new Interpolators.Linear();
    TimeValue start = new TimeValue(1, 5);
    TimeValue end = new TimeValue(5, 3);
    Assert.assertEquals(5, interpolator.interpolate(start, end, 1));
    Assert.assertEquals(5, interpolator.interpolate(start, end, 2));
    Assert.assertEquals(4, interpolator.interpolate(start, end, 3));
    Assert.assertEquals(4, interpolator.interpolate(start, end, 4));
    Assert.assertEquals(3, interpolator.interpolate(start, end, 5));
  }

  @Test
  public void testBigSlopeLinearInterpolator() {
    Interpolator interpolator = new Interpolators.Linear();
    TimeValue start = new TimeValue(1, 100);
    TimeValue end = new TimeValue(5, 500);
    Assert.assertEquals(100, interpolator.interpolate(start, end, 1));
    Assert.assertEquals(200, interpolator.interpolate(start, end, 2));
    Assert.assertEquals(300, interpolator.interpolate(start, end, 3));
    Assert.assertEquals(400, interpolator.interpolate(start, end, 4));
    Assert.assertEquals(500, interpolator.interpolate(start, end, 5));
  }

  @Test
  public void testInterpolateLimit() {
    long limit = 20;
    Interpolator interpolator = new Interpolators.Step(limit);
    TimeValue start = new TimeValue(0, 10);
    TimeValue end = new TimeValue(limit + 1, 50);
    // time between points is greater than this limit, values in between should be 0
    for (int i = 1; i < 1 + limit; i++) {
      Assert.assertEquals(0, interpolator.interpolate(start, end, i));
    }

    // time between points is not greater than the limit, values in between should be interpolated
    end = new TimeValue(limit, 50);
    for (int i = 1; i < limit; i++) {
      Assert.assertEquals(10, interpolator.interpolate(start, end, i));
    }
  }
}
