/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Longs;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Combine values from multiple TimeValue Iterators.
 */
public final class TimeValueAggregator implements Iterable<TimeValue> {

  private final Collection<? extends Iterable<TimeValue>> timeValues;

  public TimeValueAggregator(Collection<? extends Iterable<TimeValue>> timeValues) {
    this.timeValues = ImmutableList.copyOf(timeValues);
  }

  @Override
  public Iterator<TimeValue> iterator() {
    final List<PeekingIterator<TimeValue>> iterators = Lists.newLinkedList();

    for (Iterable<TimeValue> timeValue : timeValues) {
      iterators.add(Iterators.peekingIterator(timeValue.iterator()));
    }

    return new AbstractIterator<TimeValue>() {
      @Override
      protected TimeValue computeNext() {
        long timestamp = Long.MAX_VALUE;

        // Find the lowest timestamp
        boolean found = false;
        Iterator<PeekingIterator<TimeValue>> timeValuesItor = iterators.iterator();
        while (timeValuesItor.hasNext()) {
          PeekingIterator<TimeValue> iterator = timeValuesItor.next();

          // Remove iterators that have no values.
          if (!iterator.hasNext()) {
            timeValuesItor.remove();
            continue;
          }

          long ts = iterator.peek().getTime();
          if (ts <= timestamp) {
            timestamp = ts;
            found = true;
          }
        }
        if (!found) {
          return endOfData();
        }

        // Aggregates values from the same timestamp
        int value = 0;
        timeValuesItor = iterators.iterator();
        while (timeValuesItor.hasNext()) {
          PeekingIterator<TimeValue> iterator = timeValuesItor.next();
          if (iterator.peek().getTime() == timestamp) {
            value += iterator.next().getValue();
          }
        }

        return new TimeValue(timestamp, value);
      }
    };
  }

  private static final class TimeValueIteratorComparator implements Comparator<PeekingIterator<TimeValue>> {

    @Override
    public int compare(PeekingIterator<TimeValue> o1, PeekingIterator<TimeValue> o2) {
      if (o1.hasNext() && o2.hasNext()) {
        return Longs.compare(o1.peek().getTime(), o2.peek().getTime());
      }
      return o1.hasNext() ? -1 : 1;
    }
  }
}
