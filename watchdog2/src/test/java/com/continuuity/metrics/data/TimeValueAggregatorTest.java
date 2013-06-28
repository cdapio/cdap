/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.google.common.collect.Lists;
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
    for (TimeValue tv : aggregator) {
      System.out.println(tv.getTimestamp() + " " + tv.getValue());
    }
  }
}
