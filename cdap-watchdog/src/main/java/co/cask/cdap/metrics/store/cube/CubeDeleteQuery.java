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

package co.cask.cdap.metrics.store.cube;

import co.cask.cdap.metrics.store.timeseries.MeasureType;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Query that specifies delete entries from cube
 */
public class CubeDeleteQuery {

  private final long startTs;
  private final long endTs;
  private final int limit;
  private final String measureName;
  // todo: should be aggregation? e.g. also support min/max, etc.
  private final MeasureType measureType;
  private final Map<String, String> sliceByTagValues;
  private final boolean measurePrefixMatch;

  public CubeDeleteQuery(long startTs, long endTs, String measureName, MeasureType measureType,
                         Map<String, String> sliceByTagValues, boolean measurePrefixMatch) {
    this(startTs, endTs, -1,
         measureName, measureType,
         sliceByTagValues, measurePrefixMatch);
  }

  public CubeDeleteQuery(long startTs, long endTs, int limit, String measureName,
                         MeasureType measureType, Map<String, String> sliceByTagValues, boolean measurePrefixMatch) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.limit = limit;
    this.measureName = measureName;
    this.measureType = measureType;
    this.sliceByTagValues = ImmutableMap.copyOf(sliceByTagValues);
    this.measurePrefixMatch = measurePrefixMatch;

  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  // todo: push down limit support to Cube
  public int getLimit() {
    return limit;
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

  public boolean isMeasurePrefixMatch() {
    return measurePrefixMatch;
  }


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("startTs", startTs)
      .add("endTs", endTs)
      .add("measureName", measureName)
      .add("measureType", measureType)
      .add("sliceByTags", Joiner.on(",").withKeyValueSeparator(":").useForNull("null").join(sliceByTagValues))
      .toString();
  }
}
