/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.metrics.data.TimeValue;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Represents timeseries data response to REST calls. Use {@link #builder(long, long)}
 * to construct and GSON to serialize.
 */
final class TimeSeriesResponse {

  private final long start;
  private final long end;
  private final List<TimeValue> data;

  public static Builder builder(final long start, final long end) {
    final ImmutableList.Builder<TimeValue> timeValues = ImmutableList.builder();

    return new Builder() {
      @Override
      public Builder addData(long timestamp, int value) {
        return addData(new TimeValue(timestamp, value));
      }

      @Override
      public Builder addData(TimeValue timeValue) {
        timeValues.add(timeValue);
        return this;
      }

      @Override
      public TimeSeriesResponse build() {
        return new TimeSeriesResponse(start, end, timeValues.build());
      }
    };
  }

  private TimeSeriesResponse(long start, long end, List<TimeValue> data) {
    this.start = start;
    this.end = end;
    this.data = data;
  }

  public interface Builder {
    Builder addData(long timestamp, int value);

    Builder addData(TimeValue timeValue);

    TimeSeriesResponse build();
  }
}
