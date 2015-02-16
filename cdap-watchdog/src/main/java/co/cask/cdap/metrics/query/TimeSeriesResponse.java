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
package co.cask.cdap.metrics.query;

import co.cask.cdap.metrics.store.timeseries.TimeValue;
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
      public Builder addData(long timestamp, long value) {
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
    Builder addData(long timestamp, long value);

    Builder addData(TimeValue timeValue);

    TimeSeriesResponse build();
  }
}
