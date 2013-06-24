/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Longs;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

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
    final MinMaxPriorityQueue<PeekingIterator<TimeValue>> timeValueQueue =
      MinMaxPriorityQueue.orderedBy(new TimeValueIteratorComparator())
                         .expectedSize(timeValues.size())
                         .create();

    for (Iterable<TimeValue> timeValue : timeValues) {
      timeValueQueue.add(Iterators.peekingIterator(timeValue.iterator()));
    }

    return new AbstractIterator<TimeValue>() {
      @Override
      protected TimeValue computeNext() {
        // Find the first non empty iterator with the lowest timestamp
        PeekingIterator<TimeValue> iterator = timeValueQueue.peekFirst();
        while (!timeValueQueue.isEmpty() && !iterator.hasNext()) {
          timeValueQueue.removeFirst();
          iterator = timeValueQueue.peekFirst();
        }

        if (timeValueQueue.isEmpty()) {
          return endOfData();
        }

        timeValueQueue.removeFirst();
        TimeValue timeValue = iterator.next();
        timeValueQueue.add(iterator);

        long ts = timeValue.getTimestamp();
        int aggregate = timeValue.getValue();

        // Aggregate all iterators that has the same timestamp
        iterator = timeValueQueue.peekFirst();
        while (iterator.hasNext() && iterator.peek().getTimestamp() == ts) {
          timeValueQueue.removeFirst();
          TimeValue tv = iterator.next();
          timeValueQueue.add(iterator);
          aggregate += tv.getValue();
          iterator = timeValueQueue.peekFirst();
        }

        return new TimeValue(ts, aggregate);
      }
    };
  }

  private static final class TimeValueIteratorComparator implements Comparator<PeekingIterator<TimeValue>> {

    @Override
    public int compare(PeekingIterator<TimeValue> o1, PeekingIterator<TimeValue> o2) {
      if (o1.hasNext() && o2.hasNext()) {
        return Longs.compare(o1.peek().getTimestamp(), o2.peek().getTimestamp());
      }
      return o1.hasNext() ? -1 : 1;
    }
  }
}
