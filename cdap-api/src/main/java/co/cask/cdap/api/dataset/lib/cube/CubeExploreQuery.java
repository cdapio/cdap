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

package co.cask.cdap.api.dataset.lib.cube;

import co.cask.cdap.api.annotation.Beta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Defines a query to perform exploration of the {@link Cube} data, e.g. to find dimension name and values and
 * measure names.
 */
@Beta
public class CubeExploreQuery {
  private final long startTs;
  private final long endTs;
  private final int resolution;
  private final int limit;
  private final List<DimensionValue> dimensionValues;

  /**
   * Creates instance of {@link CubeExploreQuery} that defines selection of data of {@link Cube} to explore in.
   * @param startTs start time of the data selection, inclusive, in seconds since epoch
   * @param endTs end time of the data selection, exclusive, in seconds since epoch
   * @param resolution resolution of the aggregations explore
   * @param dimensionValues dimension name, dimension value pairs that define the data selection
   */
  public CubeExploreQuery(long startTs, long endTs, int resolution, int limit, List<DimensionValue> dimensionValues) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.dimensionValues = Collections.unmodifiableList(new ArrayList<DimensionValue>(dimensionValues));
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

  public int getLimit() {
    return limit;
  }

  public List<DimensionValue> getDimensionValues() {
    return dimensionValues;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CubeExploreQuery");
    sb.append("{startTs=").append(startTs);
    sb.append(", endTs=").append(endTs);
    sb.append(", resolution=").append(resolution);
    sb.append(", limit=").append(limit);
    sb.append(", dimensionValues=").append(dimensionValues);
    sb.append('}');
    return sb.toString();
  }
}
