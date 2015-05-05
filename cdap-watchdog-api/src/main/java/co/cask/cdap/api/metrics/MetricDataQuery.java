/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.api.metrics;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Interpolator;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Defines a query to perform on {@link MetricStore} data.
 * </p>
 * Though limited currently in functionality, you can map {@link MetricDataQuery} to the following statement:
 * <pre>
 * SELECT count('read.ops')                                     << metric name and aggregation function
 * FROM Cube
 * GROUP BY dataset,                                            << groupByTags
 * WHERE namespace='ns1' AND app='myApp' AND program='myFlow'   << sliceByTags
 *
 * </pre>
 */
public final class MetricDataQuery {

  /**
   * Start timestamp, in seconds.
   */
  private final long startTs;

  /**
   * End timestamp, in seconds.
   */
  private final long endTs;

  /**
   * Resolution in seconds.
   */
  private final int resolution;
  private final int limit;
  private final Map<String, AggregationFunction> metrics;
  private final Map<String, String> sliceByTagValues;
  private final List<String> groupByTags;

  private final Interpolator interpolator;

  /**
   * @param startTs Start timestamp, in seconds.
   * @param endTs End timestamp, in seconds.
   * @param resolution Resolution in seconds.
   */
  public MetricDataQuery(long startTs, long endTs, int resolution,
                         String metricName, AggregationFunction func,
                         Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this(startTs, endTs, resolution, -1, ImmutableMap.of(metricName, func), sliceByTagValues, groupByTags, null);
  }

  /**
   * @param startTs Start timestamp, in seconds.
   * @param endTs End timestamp, in seconds.
   * @param resolution Resolution in seconds.
   */
  public MetricDataQuery(long startTs, long endTs, int resolution,
                         Map<String, AggregationFunction> metrics,
                         Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this(startTs, endTs, resolution, -1, metrics, sliceByTagValues, groupByTags, null);
  }

  /**
   * @param startTs Start timestamp, in seconds.
   * @param endTs End timestamp, in seconds.
   * @param resolution Resolution in seconds.
   */
  public MetricDataQuery(long startTs, long endTs, int resolution, int limit,
                         Map<String, AggregationFunction> metrics,
                         Map<String, String> sliceByTagValues, List<String> groupByTags,
                         @Nullable Interpolator interpolator) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.metrics = metrics;
    this.sliceByTagValues = Maps.newHashMap(sliceByTagValues);
    this.groupByTags = ImmutableList.copyOf(groupByTags);
    this.interpolator = interpolator;
  }

  public MetricDataQuery(MetricDataQuery query, String metricName, AggregationFunction func) {
    this(query.startTs, query.endTs, query.resolution, query.limit,
         ImmutableMap.of(metricName, func),
         query.sliceByTagValues, query.groupByTags, query.getInterpolator());
  }

  public MetricDataQuery(MetricDataQuery query, Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this(query.startTs, query.endTs, query.resolution, query.limit, query.metrics,
         sliceByTagValues, groupByTags, query.getInterpolator());
  }

  public MetricDataQuery(MetricDataQuery query, Map<String, String> sliceByTagValues) {
    this(query.startTs, query.endTs, query.resolution, query.limit, query.metrics,
         sliceByTagValues, query.groupByTags, query.getInterpolator());
  }

  public MetricDataQuery(MetricDataQuery query, List<String> groupByTags) {
    this(query.startTs, query.endTs, query.resolution, query.limit, query.metrics,
         query.sliceByTagValues, groupByTags, query.getInterpolator());
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public int getResolution() {
    return resolution;
  }

  public Map<String, AggregationFunction> getMetrics() {
    return metrics;
  }

  public Map<String, String> getSliceByTags() {
    return sliceByTagValues;
  }

  public List<String> getGroupByTags() {
    return groupByTags;
  }

  // todo: push down limit support to Cube
  public int getLimit() {
    return limit;
  }

  public Interpolator getInterpolator() {
    return interpolator;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("startTs", startTs)
      .add("endTs", endTs)
      .add("resolution", resolution)
      .add("metrics", metrics)
      .add("sliceByTags", Joiner.on(",").withKeyValueSeparator(":").useForNull("null").join(sliceByTagValues))
      .add("groupByTags", Joiner.on(",").join(groupByTags)).toString();
  }
}
