/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Query that specifies parameters to delete entries from {@link MetricStore}.
 */
public class MetricDeleteQuery {
  private final long startTs;
  private final long endTs;
  private final List<String> metricNames;
  private final Map<String, String> sliceByTagValues;
  private final List<String> aggregationTags;

  /**
   * Creates instance of {@link MetricDeleteQuery} that defines selection of data to delete from the metric store.
   *
   * @param startTs start time of the data selection, in seconds since epoch
   * @param endTs end time of the data selection, in seconds since epoch
   * @param metricNames ame of the metric names to delete, empty collection means delete all
   * @param sliceByTagValues the key value pair of the tag and value
   * @param aggregationTags list of tags for the metric aggregation group, the order must be same as the prefix of the
   *                        aggregation groups we defined in {@link MetricStore}
   */
  public MetricDeleteQuery(long startTs, long endTs, Collection<String> metricNames,
                           Map<String, String> sliceByTagValues, List<String> aggregationTags) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.metricNames = new ArrayList<>(metricNames);
    this.sliceByTagValues = new LinkedHashMap<>(sliceByTagValues);
    this.aggregationTags = new ArrayList<>(aggregationTags);
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public Collection<String> getMetricNames() {
    return metricNames;
  }

  public Map<String, String> getSliceByTags() {
    return sliceByTagValues;
  }

  public Predicate<List<String>> getTagPredicate() {
    return aggregates -> Collections.indexOfSubList(aggregates, aggregationTags) == 0;
  }

  @Override
  public String toString() {
    return "MetricDeleteQuery{" +
      "startTs=" + startTs +
      ", endTs=" + endTs +
      ", metricNames=" + metricNames +
      ", sliceByTagValues=" + sliceByTagValues +
      ", aggregationTags=" + aggregationTags +
      '}';
  }
}
