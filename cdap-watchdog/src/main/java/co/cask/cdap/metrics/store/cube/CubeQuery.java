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

package co.cask.cdap.metrics.store.cube;

import co.cask.cdap.metrics.store.timeseries.MeasureType;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Defines a query to perform on {@link Cube} data.
 * </p>
 * Though limited currently in functionality, you can map {@link CubeQuery} to the following statement:
 * <pre>
 * SELECT count('read.ops')                                     << measure name and type
 * FROM Cube
 * GROUP BY dataset,                                            << groupByTags
 * WHERE namespace='ns1' AND app='myApp' AND program='myFlow'   << sliceByTags
 *
 * </pre>
 * See also {@link Cube#query(CubeQuery)}.
 */
// todo: should support interpolator
public final class CubeQuery {
  private final long startTs;
  private final long endTs;
  private final int resolution;
  private final int limit;
  private final String measureName;
  // todo: should be aggregation? e.g. also support min/max, etc.
  private final MeasureType measureType;
  private final Map<String, String> sliceByTagValues;
  private final List<String> groupByTags;

  public CubeQuery(long startTs, long endTs, int resolution,
                   String measureName, MeasureType measureType,
                   Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this(startTs, endTs, resolution, -1,
         measureName, measureType,
         sliceByTagValues, groupByTags);
  }

  public CubeQuery(long startTs, long endTs, int resolution, int limit,
                   String measureName, MeasureType measureType,
                   Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.measureName = measureName;
    this.measureType = measureType;
    this.sliceByTagValues = ImmutableMap.copyOf(sliceByTagValues);
    this.groupByTags = ImmutableList.copyOf(groupByTags);
  }

  public CubeQuery(CubeQuery query, String measureName) {
    this(query.startTs, query.endTs, query.resolution, query.limit,
         measureName, query.measureType,
         query.sliceByTagValues, query.groupByTags);
  }

  public CubeQuery(CubeQuery query, Map<String, String> sliceByTagValues) {
    this(query.startTs, query.endTs, query.resolution, query.limit,
         query.measureName, query.measureType,
         sliceByTagValues, query.groupByTags);
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

  public String getMeasureName() {
    return measureName;
  }

  public MeasureType getMeasureType() {
    return measureType;
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("startTs", startTs)
      .add("endTs", endTs)
      .add("resolution", resolution)
      .add("measureName", measureName)
      .add("measureType", measureType)
      .add("sliceByTags", Joiner.on(",").withKeyValueSeparator(":").useForNull("null").join(sliceByTagValues))
      .add("groupByTags", Joiner.on(",").join(groupByTags)).toString();
  }
}
