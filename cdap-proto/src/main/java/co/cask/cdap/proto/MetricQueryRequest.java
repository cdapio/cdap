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

package co.cask.cdap.proto;

import co.cask.cdap.api.dataset.lib.cube.Interpolator;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Metrics Query Request format
 */
public class MetricQueryRequest {
  /**
   * Format for metrics query in batched queries
   */
  Map<String, String> tags;
  List<String> metrics;
  List<String> groupBy;
  TimeRange timeRange;

  public MetricQueryRequest(Map<String, String> tags, List<String> metrics, List<String> groupBy) {
    this.tags = tags;
    this.metrics = metrics;
    this.groupBy = groupBy;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public List<String> getGroupBy() {
    return groupBy;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public void setTimeRange(@Nullable Long start, @Nullable Long end, @Nullable Integer count,
                           @Nullable Integer resolution,  @Nullable Interpolator interpolator) {
    timeRange = new TimeRange(start, end, count, resolution, interpolator);
  }

  /**
   * Represents the time range of the query request
   */
  public class TimeRange {
    private Long startTs;
    private Long endTs;
    private Integer count;
    private Integer resolutionInSeconds;
    private Interpolator interpolator;

    public TimeRange(Long start, Long end, Integer count, Integer resolutionInSeconds, Interpolator interpolator) {
      this.startTs = start;
      this.endTs = end;
      this.count = count;
      this.resolutionInSeconds = resolutionInSeconds;
      this.interpolator = interpolator;
    }

    public Interpolator getInterpolate() {
      return interpolator;
    }

    public Integer getResolutionInSeconds() {
      return resolutionInSeconds;
    }

    public Integer getCount() {
      return count;
    }

    public Long getEnd() {
      return endTs;
    }

    public Long getStart() {
      return startTs;
    }
  }
}
