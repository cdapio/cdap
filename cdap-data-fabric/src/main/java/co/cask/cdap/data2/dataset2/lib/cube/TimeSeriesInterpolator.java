/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.lib.cube.Interpolator;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Collection;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Applies given interpolator to a given time series,
 * Given a timeseries, interpolates the values at each timestamp between the earliest and
 * latest data points if there is no data point available at that timestamp
 * and the value at the timestamp can be interpolated.
 *
 * Interpolating the data just means we're filling in the missing points with something that
 * is reasonably likely to have been the true value at that point.
 *
 * With linear interpolation, the individual time series get transformed into:
 *   t1  t2  t3  t4  t5
 *   5   -   -   -   3
 *   5   5   4   4   3
 *
 * With step interpolation, the time series get transformed into:
 *   t1  t2  t3  t4  t5
 *   5   -   -   -   3
 *   5   5   5   5   3
 */
class TimeSeriesInterpolator implements Iterable<TimeValue> {

  private final Collection<TimeValue> timeSeries;
  @Nullable
  private final Interpolator interpolator;
  private final int resolution;

  public TimeSeriesInterpolator(Collection<TimeValue> timeValues,
                                @Nullable Interpolator interpolator, int resolution) {
    this.timeSeries = ImmutableList.copyOf(timeValues);
    this.interpolator = interpolator;
    this.resolution = resolution;
  }

  @Override
  public Iterator<TimeValue> iterator() {
    return new InterpolatedAggregatorIterator();
  }

  private class InterpolatedAggregatorIterator extends AbstractIterator<TimeValue> {

    private long currentTs;
    BiDirectionalPeekingIterator timeseries;

    InterpolatedAggregatorIterator() {
      timeseries = new BiDirectionalPeekingIterator(Iterators.peekingIterator(timeSeries.iterator()));
      if (timeseries.hasNext()) {
        currentTs = timeseries.peek().getTimestamp();
      }
    }

    @Override
    protected TimeValue computeNext() {
      long currentTsValue = 0;
      // no more data points in the timeseries
      if (!timeseries.hasNext()) {
        return endOfData();
      }

      // move the iterator to the next point in this timeseries if this is an actual data point and not interpolated.
      if (timeseries.peek().getTimestamp() == currentTs) {
        currentTsValue += timeseries.peek().getValue();
        timeseries.next();
      } else if (interpolator != null && timeseries.peekBefore() != null) {
        // don't interpolate unless we're in between data points
        currentTsValue += interpolator.interpolate(timeseries.peekBefore(), timeseries.peek(), currentTs);
      }

      TimeValue output = new TimeValue(currentTs, currentTsValue);
      if (timeseries.hasNext()) {
        // increment the currentTs by resolution to get the next data point.
        currentTs = (interpolator == null) ? timeseries.peek().getTimestamp() : currentTs + resolution;
      }
      return output;
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
}
