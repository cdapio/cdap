/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class TimeValueAggregatorTest {


  @Test
  public void testSimpleNoInterpolationAggregate() {
    List<List<TimeValue>> list = Lists.newArrayList();

    List<TimeValue> timeValues = Lists.newArrayList();
    timeValues.add(new TimeValue(1, 5));
    timeValues.add(new TimeValue(10, 5));
    list.add(timeValues);
    List<TimeValue> timeValues2 = Lists.newArrayList();
    timeValues2.add(new TimeValue(5, 1));
    list.add(timeValues2);

    TimeValueAggregator aggregator = new TimeValueAggregator(list, null);
    Iterator<TimeValue> aggTimeValues = aggregator.iterator();
    TimeValue tv = aggTimeValues.next();
    Assert.assertEquals(1, tv.getTime());
    Assert.assertEquals(5, tv.getValue());
    tv = aggTimeValues.next();
    Assert.assertEquals(5, tv.getTime());
    Assert.assertEquals(1, tv.getValue());
    tv = aggTimeValues.next();
    Assert.assertEquals(10, tv.getTime());
    Assert.assertEquals(5, tv.getValue());
    Assert.assertFalse(aggTimeValues.hasNext());
  }

  @Test
  public void testNoInterpolationAggregate() {
    List<List<TimeValue>> list = Lists.newArrayList();

    for (int i = 0; i < 5; i++) {
      List<TimeValue> timeValues = Lists.newArrayList();
      for (int j = 0; j < 10; j++) {
        timeValues.add(new TimeValue(j, i + j));
      }
      list.add(timeValues);
    }

    TimeValueAggregator aggregator = new TimeValueAggregator(list, null);
    int ts = 0;
    for (TimeValue tv : aggregator) {
      Assert.assertEquals(ts, tv.getTime());
      // The value is (ts + (ts + 1) + (ts + 2) + (ts + 3) + (ts + 4))
      Assert.assertEquals((ts + (ts + 4)) * 5 / 2, tv.getValue());
      ts++;
    }
  }

  /**
   * Given two series that look like:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   -   4   -   6   -   8
   *   1   -   3   -   5   -   7   -
   *
   * The step interpolation, the individual time series get transformed into:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   2   4   4   6   6   8
   *   1   1   3   3   5   5   7   -
   *
   * and the final aggregate timeseries becomes:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   1   3   5   7   9   11  13  8
   */
  @Test
  public void testStepInterpolatedAggregator() {
    List<List<TimeValue>> list = generateInput();

    TimeValueAggregator aggregator =
      new TimeValueAggregator(list, new Interpolators.Step());
    int numPoints = 0;
    for (TimeValue tv : aggregator) {
      // first point doesn't follow the same formula, check it separately.
      if (tv.getTime() == 8) {
        Assert.assertEquals(8, tv.getValue());
      } else {
        Assert.assertEquals(2 * tv.getTime() - 1, tv.getValue());
      }
      numPoints++;
    }
    Assert.assertEquals(8, numPoints);
  }


  /**
   * Given two series that look like:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   -   4   -   6   -   8
   *   1   -   3   -   5   -   7   -
   *
   * The step interpolation, the individual time series get transformed into:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   3   4   5   6   7   8
   *   1   2   3   4   5   6   7   -
   *
   * and the final aggregate timeseries becomes:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   1   4   6   8   10  12  14  8
   */
  @Test
  public void testLinearInterpolatedAggregator() {
    List<List<TimeValue>> list = generateInput();

    TimeValueAggregator aggregator =
      new TimeValueAggregator(list, new Interpolators.Linear());
    int numPoints = 0;
    for (TimeValue tv : aggregator) {
      // timestamps 1 and 8 only has one datapoint, so it doesn't follow the pattern.
      if (tv.getTime() == 1L || tv.getTime() == 8L) {
        Assert.assertEquals(tv.getValue(), tv.getTime());
      } else {
        Assert.assertEquals(2 * tv.getTime(), tv.getValue());
      }
      numPoints++;
    }
    Assert.assertEquals(8, numPoints);
  }

  private List<List<TimeValue>> generateInput() {
    List<List<TimeValue>> list = Lists.newArrayList();

    // even numbers
    List<TimeValue> timeseries1 = Lists.newLinkedList();
    for (int i = 2; i <= 8; i += 2) {
      timeseries1.add(new TimeValue(i, i));
    }
    list.add(timeseries1);

    // odd numbers
    List<TimeValue> timeseries2 = Lists.newLinkedList();
    for (int i = 1; i <= 7; i += 2) {
      timeseries2.add(new TimeValue(i, i));
    }
    list.add(timeseries2);
    return list;
  }
}
