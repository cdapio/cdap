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

package io.cdap.cdap.api.dataset.lib.cube;

import io.cdap.cdap.api.annotation.Beta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Defines a query for deleting data in {@link Cube}.
 */
@Beta
public class CubeDeleteQuery {
  private final long startTs;
  private final long endTs;
  private final int resolution;
  private final Collection<String> measureNames;
  private final Map<String, String> dimensionValues;
  private final Predicate<List<String>> tagPredicate;

  /**
   * Creates instance of {@link CubeDeleteQuery} that defines selection of data to delete from {@link Cube}.
   * @param startTs start time of the data selection, in seconds since epoch
   * @param endTs end time of the data selection, in seconds since epoch
   * @param resolution resolution of the aggregations to delete from
   * @param dimensionValues dimension name, dimension value pairs that define the data selection
   * @param measureNames name of the measures to delete, {@code null} means delete all
   * @param tagPredicate predicate to decide how to match the aggregation group to the given tags
   */
  public CubeDeleteQuery(long startTs, long endTs, int resolution,
                         Map<String, String> dimensionValues, Collection<String> measureNames,
                         Predicate<List<String>> tagPredicate) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.measureNames = Collections.unmodifiableCollection(new ArrayList<>(measureNames));
    this.dimensionValues = Collections.unmodifiableMap(new LinkedHashMap<>(dimensionValues));
    this.tagPredicate = tagPredicate;
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

  public Collection<String> getMeasureNames() {
    return measureNames;
  }

  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public Predicate<List<String>> getTagPredicate() {
    return tagPredicate;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CubeDeleteQuery");
    sb.append("{startTs=").append(startTs);
    sb.append(", endTs=").append(endTs);
    sb.append(", resolution=").append(resolution);
    sb.append(", measureNames=").append(measureNames == null ? "null" : measureNames);
    sb.append(", dimensionValues=").append(dimensionValues);
    sb.append('}');
    return sb.toString();
  }
}
