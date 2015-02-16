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

package co.cask.cdap.metrics.query;

import java.util.Map;

/**
 * Represents metric query result. This is used for decorating REST API output.
 */
// todo: move to cdap-proto along with adding CLI support for metrics querying
public final class MetricQueryResult {
  private final long startTs;
  private final long endTs;
  private final TimeSeries[] serieses;

  MetricQueryResult(long startTs, long endTs, TimeSeries[] serieses) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.serieses = serieses;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public TimeSeries[] getSerieses() {
    return serieses;
  }

  public static final class TimeSeries {
    private final String metricName;
    private final Map<String, String> grouping;
    private final TimeValue[] data;

    TimeSeries(String metricName, Map<String, String> grouping, TimeValue[] data) {
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

  public static final class TimeValue {
    private final long time;
    private final long value;

    TimeValue(long time, long value) {
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