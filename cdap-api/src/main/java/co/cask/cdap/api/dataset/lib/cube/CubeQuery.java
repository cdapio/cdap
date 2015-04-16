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

import java.util.ArrayList;
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
 * SELECT count('read.ops')                                     << measure name and type
 * FROM 1min_resolution                                         << resolution
 * GROUP BY dataset,                                            << groupByTags
 * WHERE namespace='ns1' AND app='myApp' AND program='myFlow'   << sliceByTags
 * LIMIT 100                                                    << limit
 *
 * </pre>
 * See also {@link Cube#query(CubeQuery)}.
 */
@Beta
public final class CubeQuery {
  private final long startTs;
  private final long endTs;
  private final int resolution;
  private final int limit;
  private final String measureName;
  private final MeasureType measureType;
  private final Map<String, String> sliceByTagValues;
  private final List<String> groupByTags;
  private final Interpolator interpolator;

  public CubeQuery(long startTs, long endTs, int resolution, int limit,
                   String measureName, MeasureType measureType,
                   Map<String, String> sliceByTagValues, List<String> groupByTags) {
    this(startTs, endTs, resolution, limit,
         measureName, measureType,
         sliceByTagValues, groupByTags, null);
  }

  public CubeQuery(long startTs, long endTs, int resolution, int limit,
                   String measureName, MeasureType measureType,
                   Map<String, String> sliceByTagValues, List<String> groupByTags,
                   @Nullable Interpolator interpolator) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.measureName = measureName;
    this.measureType = measureType;
    this.sliceByTagValues = Collections.unmodifiableMap(new HashMap<String, String>(sliceByTagValues));
    this.groupByTags = Collections.unmodifiableList(new ArrayList<String>(groupByTags));
    this.interpolator = interpolator;
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

  public Interpolator getInterpolator() {
    return interpolator;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CubeQuery");
    sb.append("{startTs=").append(startTs);
    sb.append(", endTs=").append(endTs);
    sb.append(", resolution=").append(resolution);
    sb.append(", limit=").append(limit);
    sb.append(", measureName='").append(measureName).append('\'');
    sb.append(", measureType=").append(measureType);
    sb.append(", sliceByTagValues=").append(sliceByTagValues);
    sb.append(", groupByTags=").append(groupByTags);
    sb.append(", interpolator=").append(interpolator);
    sb.append('}');
    return sb.toString();
  }
}
