package com.continuuity.metrics.data;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Given a collection of timeseries, aggregate the values at each timestamp between the earliest and
 * latest data points, where the value at each timestamp can be interpolated if there is not a datapoint at
 * that timestamp.  For example, given two series that look like:
 *   t1  t2  t3  t4  t5  t6  t7  t8
 *   -   5   -   5   -   5   -   9
 *   1   -   3   -   1   -   3   -
 *
 * without any interpolation, it would aggregate to a single timeseries like:
 *   t1  t2  t3  t4  t5  t6  t7  t8
 *   1   5   3   5   1   5   3   9
 *
 * This is fine if the absence of data really means the value there is a 0.  However,
 * if there is no data because we are not writing data points at the finest granularity (1 second), then this
 * aggregate does not give an accurate picture.  This can be the case if we're sampling, or
 * if the metric being tracked does not change very frequently, and is thus not written frequently.
 * Interpolating the data just means we're filling in the missing points with something that
 * is reasonably likely to have been the true value at that point.
 *
 * With linear interpolation, the individual time series get transformed into:
 *   t1  t2  t3  t4  t5  t6  t7  t8
 *   -   5   5   5   5   5   7   9
 *   1   2   3   2   1   2   3   -
 *
 * and the final aggregate timeseries becomes:
 *   t1  t2  t3  t4  t5  t6  t7  t8
 *   1   7   8   7   6   7   10  9
 *
 * With step interpolation, the individual time series get transformed into:
 *   t1  t2  t3  t4  t5  t6  t7  t8
 *   -   5   5   5   5   5   5   9
 *   1   1   3   3   1   1   3   -
 *
 * and the final aggregate timeseries becomes:
 *   t1  t2  t3  t4  t5  t6  t7  t8
 *   1   6   8   8   6   6   8   9
 */
public class TimeValueAggregator implements Iterable<TimeValue> {

  private final Collection<? extends Iterable<TimeValue>> allTimeseries;
  private final Interpolator interpolator;

  public TimeValueAggregator(Collection<? extends Iterable<TimeValue>> timeValues) {
    this(timeValues, null);
  }

  public TimeValueAggregator(Collection<? extends Iterable<TimeValue>> timeValues, Interpolator interpolator) {
    this.allTimeseries = ImmutableList.copyOf(timeValues);
    this.interpolator = interpolator;
  }

  @Override
  public Iterator<TimeValue> iterator() {
    return new InterpolatedAggregatorIterator();
  }

  private class InterpolatedAggregatorIterator extends AbstractIterator<TimeValue> {
    private final List<BiDirectionalPeekingIterator> timeseriesList;
    private long currentTs;

    InterpolatedAggregatorIterator() {
      this.timeseriesList = Lists.newLinkedList();
      for (Iterable<TimeValue> timeseries : allTimeseries) {
        timeseriesList.add(new BiDirectionalPeekingIterator(Iterators.peekingIterator(timeseries.iterator())));
      }
      this.currentTs = findEarliestTimestamp();
    }

    @Override
    protected TimeValue computeNext() {
      // if we started out with empty timeseries
      if (currentTs == Long.MAX_VALUE) {
        return endOfData();
      }

      // Aggregates values from the same timestamp, using interpolated values if there is not data at the timestamp.
      boolean atEnd = true;
      int currentTsValue = 0;
      Iterator<BiDirectionalPeekingIterator> timeseriesIter = timeseriesList.iterator();
      while (timeseriesIter.hasNext()) {
        BiDirectionalPeekingIterator timeseries = timeseriesIter.next();

        // no more data points in this timeseries, remove it
        if (!timeseries.hasNext()) {
          timeseriesIter.remove();
          continue;
        }

        // we're not at the end unless all the timeseries are out of data points
        atEnd = false;

        // move the iterator to the next point in this timeseries if this is an actual data point and not interpolated.
        if (timeseries.peek().getTime() == currentTs) {
          currentTsValue += timeseries.peek().getValue();
          timeseries.next();
        } else if (interpolator != null && timeseries.peekBefore() != null) {
          // don't interpolate unless we're in between data points
          currentTsValue += interpolator.interpolate(timeseries.peekBefore(), timeseries.peek(), currentTs);
        }
      }

      if (atEnd) {
        return endOfData();
      }
      TimeValue output = new TimeValue(currentTs, currentTsValue);
      currentTs = (interpolator == null) ? findEarliestTimestamp() : currentTs + 1;
      return output;
    }

    // find the first timestamp across all timeseries
    long findEarliestTimestamp() {
      long earliest = Long.MAX_VALUE;
      Iterator<BiDirectionalPeekingIterator> timeseriesIter = timeseriesList.iterator();
      // go through each separate timeseries, and look for the one that has the earliest first timestamp.
      while (timeseriesIter.hasNext()) {
        BiDirectionalPeekingIterator timeseries = timeseriesIter.next();

        // no data points in this timeseries, doesn't need to be here so remove it
        if (!timeseries.hasNext()) {
          timeseriesIter.remove();
          continue;
        }

        long timeseriesEarliest = timeseries.peek().getTime();
        if (timeseriesEarliest < earliest) {
          earliest = timeseriesEarliest;
        }
      }
      return earliest;
    }
  }

  /**
   * Iterator that allows peeking on the next value and also retrieving the last value, which will be null
   * if there was no last value.  Used to allow interpolating between two datapoints in a timeseries.
   */
  public final class BiDirectionalPeekingIterator implements PeekingIterator<TimeValue> {
    PeekingIterator<TimeValue> iter;
    TimeValue lastValue;

    public BiDirectionalPeekingIterator(PeekingIterator<TimeValue> iter) {
      this.iter = iter;
      this.lastValue = null;
    }

    @Override
    public TimeValue peek() {
      return iter.peek();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public TimeValue next() {
      lastValue = iter.next();
      return lastValue;
    }

    @Override
    public void remove() {
      iter.remove();
    }

    public TimeValue peekBefore() {
      return lastValue;
    }
  }
}
