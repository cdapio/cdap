/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import java.util.Map;

/**
 * Represents metric query result. This is used for decorating REST API output.
 */
public final class MetricQueryResult {
  private final long startTime;
  private final long endTime;
  private final TimeSeries[] series;

  public MetricQueryResult(long startTime, long endTime, TimeSeries[] series) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.series = series;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public TimeSeries[] getSeries() {
    return series;
  }

  /**
   * Represents time series of a metric. This is used for decorating REST API output.
   */
  public static final class TimeSeries {
    private final String metricName;
    private final Map<String, String> grouping;
    private final TimeValue[] data;

    public TimeSeries(String metricName, Map<String, String> grouping, TimeValue[] data) {
      this.metricName = metricName;
      this.grouping = grouping;
      this.data = data;
    }

    public String getMetricName() {
      return metricName;
    }

    public Map<String, String> getGrouping() {
      return grouping;
    }

    public TimeValue[] getData() {
      return data;
    }
  }

  /**
   * Represents time value of a metric. This is used for decorating REST API output.
   */
  public static final class TimeValue {
    private final long time;
    private final long value;

    public TimeValue(long time, long value) {
      this.time = time;
      this.value = value;
    }

    public long getTime() {
      return time;
    }

    public long getValue() {
      return value;
    }
  }
}
