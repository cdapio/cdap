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

package co.cask.cdap.api.dataset.lib.cube;

import co.cask.cdap.api.annotation.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Defines a query to perform on {@link Cube} data.
 * </p>
 * Another way to think about the query is to map it to the following statement::
 * <pre>
 * SELECT count('read.ops')                                           << measure name and type
 * FROM aggregation1.1min_resolution                                  << aggregation and resolution
 * GROUP BY dataset,                                                  << groupByDimensions
 * WHERE namespace='ns1' AND app='myApp' AND program='myFlow' AND     << dimensionValues
 *       ts>=1423370200 AND ts{@literal<}1423398198                             << startTs and endTs
 * LIMIT 100                                                          << limit
 *
 * </pre>
 * See also {@link Cube#query(CubeQuery)}.
 */
@Beta
public final class CubeQuery {
  // null value means auto-choose aggregation based on query todo: auto-choosing may be error prone, remove it?
  @Nullable
  private final String aggregation;
  private final long startTs;
  private final long endTs;
  private final int resolution;
  private final int limit;
  private final Map<String, MeasureType> measurements;
  private final Map<String, String> dimensionValues;
  private final List<String> groupByDimensions;
  private final Interpolator interpolator;

  // todo : CDAP-2199 use builder instead of having multiple constructors
  /**
   * Same as {@link CubeQuery#CubeQuery(String, long, long, int, int, Map, Map, List, Interpolator)},
   * with {@code aggregation=null} and {@code interpolator=null}.
   */
  public CubeQuery(long startTs, long endTs, int resolution, int limit, Map<String, MeasureType> measurements,
                   Map<String, String> dimensionValues, List<String> groupByDimensions) {

    this(null, startTs, endTs, resolution, limit, measurements,
         dimensionValues, groupByDimensions, null);
  }

  /**
   * Same as {@link CubeQuery#CubeQuery(String, long, long, int, int, Map, Map, List, Interpolator)},
   * with {@code aggregation=null}.
   */
  public CubeQuery(long startTs, long endTs, int resolution, int limit, Map<String, MeasureType> measurements,
                   Map<String, String> dimensionValues, List<String> groupByDimensions,
                   @Nullable Interpolator interpolator) {
    this(null, startTs, endTs, resolution, limit, measurements,
         dimensionValues, groupByDimensions, interpolator);
  }

  /**
   * Creates {@link CubeQuery} with given parameters.
   * @param aggregation (optional) aggregation name to query in; if {@code null}, the aggregation will be auto-selected
   *                    based on rest of query parameters
   * @param startTs start (inclusive) of the time range to query
   * @param endTs end (exclusive) of the time range to query
   * @param resolution resolution of the aggregation to query in
   * @param limit max number of returned data points
   * @param measurements map of measure name, measure type to query for, empty map means "all measures"
   * @param dimensionValues dimension values to filter by
   * @param groupByDimensions dimensions to group by
   * @param interpolator {@link Interpolator} to use
   */
  public CubeQuery(@Nullable String aggregation,
                   long startTs, long endTs, int resolution, int limit,
                   Map<String, MeasureType> measurements,
                   Map<String, String> dimensionValues, List<String> groupByDimensions,
                   @Nullable Interpolator interpolator) {
    this.aggregation = aggregation;
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.measurements = measurements;
    this.dimensionValues = Collections.unmodifiableMap(new HashMap<String, String>(dimensionValues));
    this.groupByDimensions = Collections.unmodifiableList(new ArrayList<String>(groupByDimensions));
    this.interpolator = interpolator;
  }


  /**
   * Same as {@link CubeQuery#CubeQuery(long, long, int, int, Map, Map, List)},
   * with single measureName.
   */
  public CubeQuery(long startTs, long endTs, int resolution, int limit,
                   String measureName, MeasureType measureType,
                   Map<String, String> dimensionValues, List<String> groupByDimensions) {
    this(startTs, endTs, resolution, limit,
         ImmutableMap.of(measureName, measureType),
         dimensionValues, groupByDimensions, null);
  }

  /**
   * Same as {@link CubeQuery#CubeQuery(long, long, int, int, Map, Map, List, Interpolator)},
   * with single measureName.
   */
  public CubeQuery(long startTs, long endTs, int resolution, int limit,
                   String measureName, MeasureType measureType,
                   Map<String, String> dimensionValues, List<String> groupByDimensions,
                   @Nullable Interpolator interpolator) {
    this(startTs, endTs, resolution, limit,
         ImmutableMap.of(measureName, measureType),
         dimensionValues, groupByDimensions, interpolator);
  }

  /**
   * Same as {@link CubeQuery#CubeQuery(String, long, long, int, int, Map, Map, List, Interpolator)},
   * with single measureName.
   */
  public CubeQuery(String aggregation, long startTs, long endTs, int resolution, int limit,
                   String measureName, MeasureType measureType,
                   Map<String, String> dimensionValues, List<String> groupByDimensions) {
    this(aggregation, startTs, endTs, resolution, limit,
         ImmutableMap.of(measureName, measureType),
         dimensionValues, groupByDimensions, null);
  }

  /**
   * Same as {@link CubeQuery#CubeQuery(String, long, long, int, int, Map, Map, List, Interpolator)},
   * with single measureName.
   */
  public CubeQuery(String aggregation, long startTs, long endTs, int resolution, int limit,
                   String measureName, MeasureType measureType,
                   Map<String, String> dimensionValues, List<String> groupByDimensions,
                   @Nullable Interpolator interpolator) {
    this(aggregation, startTs, endTs, resolution, limit,
         ImmutableMap.of(measureName, measureType),
         dimensionValues, groupByDimensions, interpolator);
  }

  @Nullable
  public String getAggregation() {
    return aggregation;
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

  public Map<String, MeasureType> getMeasurements() {
    return measurements;
  }

  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public List<String> getGroupByDimensions() {
    return groupByDimensions;
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
    final StringBuilder sb = new StringBuilder();
    sb.append("CubeQuery");
    sb.append("{aggregation=").append(aggregation);
    sb.append(", startTs=").append(startTs);
    sb.append(", endTs=").append(endTs);
    sb.append(", resolution=").append(resolution);
    sb.append(", limit=").append(limit);
    sb.append(", measurements=").append(measurements);
    sb.append(", dimensionValues=").append(dimensionValues);
    sb.append(", groupByDimensions=").append(groupByDimensions);
    sb.append(", interpolator=").append(interpolator);
    sb.append('}');
    return sb.toString();
  }
}
