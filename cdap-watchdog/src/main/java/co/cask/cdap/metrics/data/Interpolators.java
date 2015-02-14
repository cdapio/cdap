/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.data;

import co.cask.cdap.metrics.store.timeseries.TimeValue;
import com.google.common.base.Preconditions;

/**
 * Returns interpolators of different types.
 */
public final class Interpolators {
  public static final long DEFAULT_MAX_ALLOWED_GAP = 60;

  public static Interpolator createDefault() {
    return new Step();
  }

  /**
   * Return 0 if the time between data points is above a give limit, or if the point to interpolate
   * is too far before the first point, or too far after the last point.
   */
  public abstract static class BaseInterpolator implements Interpolator {
    private long maxAllowedGap;

    BaseInterpolator(long maxAllowedGap) {
      this.maxAllowedGap = maxAllowedGap;
    }

    @Override
    public long interpolate(TimeValue start, TimeValue end, long ts) {
      Preconditions.checkNotNull(start);
      Preconditions.checkNotNull(end);
      Preconditions.checkArgument((ts <= end.getTimestamp()) && (ts >= start.getTimestamp()));
      // if its been too many seconds between datapoints, return a 0 for everything in between.
      if ((end.getTimestamp() - start.getTimestamp()) > maxAllowedGap) {
        return 0;
      }
      return limitedInterpolate(start, end, ts);
    }

    @Override
    public long getMaxAllowedGap() {
      return maxAllowedGap;
    }

    protected abstract long limitedInterpolate(TimeValue start, TimeValue end, long ts);
  }

  /**
   * Timestamps between 2 data points will take on the value of the previous point.
   * If the timestamp is before the start, return a 0.  If the timestamp is after the end,
   * return the end value.
   */
  public static final class Step extends BaseInterpolator {

    public Step() {
      super(DEFAULT_MAX_ALLOWED_GAP);
    }

    public Step(long maxAllowedGap) {
      super(maxAllowedGap);
    }

    @Override
    protected long limitedInterpolate(TimeValue start, TimeValue end, long ts) {
      return (ts < end.getTimestamp()) ? start.getValue() : end.getValue();
    }
  }

  /**
   * timestamps between 2 data points will increase or decrease "linearly".  If the timestamp
   * is before the start or after the end, return a 0.
   */
  public static final class Linear extends BaseInterpolator {

    public Linear() {
      super(DEFAULT_MAX_ALLOWED_GAP);
    }

    public Linear(long maxAllowedGap) {
      super(maxAllowedGap);
    }

    @Override
    protected long limitedInterpolate(TimeValue start, TimeValue end, long ts) {
      long deltaX = ts - start.getTimestamp();
      long totalX = end.getTimestamp() - start.getTimestamp();
      long totalY = end.getValue() - start.getValue();
      long deltaY = (int) (totalY * deltaX / totalX);
      return start.getValue() + deltaY;
    }
  }
}
