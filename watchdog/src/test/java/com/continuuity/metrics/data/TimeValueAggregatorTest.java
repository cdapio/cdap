/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class TimeValueAggregatorTest {

  @Test
  public void testAggregate() {
    List<List<TimeValue>> list = Lists.newArrayList();

    for (int i = 0; i < 5; i++) {
      List<TimeValue> timeValues = Lists.newArrayList();
      for (int j = 0; j < 10; j++) {
        timeValues.add(new TimeValue(j, i + j));
      }
      list.add(timeValues);
    }

    TimeValueAggregator aggregator = new TimeValueAggregator(list);
    int ts = 0;
    for (TimeValue tv : aggregator) {
      Assert.assertEquals(ts, tv.getTime());
      // The value is (ts + (ts + 1) + (ts + 2) + (ts + 3) + (ts + 4))
      Assert.assertEquals((ts + (ts + 4)) * 5 / 2, tv.getValue());
      ts++;
    }
  }
}
