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
import java.util.concurrent.TimeUnit;

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
    this.dimensionValues = Collections.unmodifiableList(new ArrayList<>(dimensionValues));
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

  /**
   * @return {@link Builder} to build {@link CubeExploreQuery}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds {@link CubeExploreQuery}.
   */
  public static final class Builder {
    private long startTs;
    private long endTs;
    private int resolution;
    private int limit;
    private List<DimensionValue> dimensionValues = new ArrayList<>();

    /**
     * @return builder for configuring {@link CubeExploreQuery}
     */
    public From from() {
      return new From();
    }

    /**
     * @return instance of {@link CubeExploreQuery}
     */
    private CubeExploreQuery build() {
      return new CubeExploreQuery(startTs, endTs, resolution, limit, dimensionValues);
    }

    /**
     * Builder for configuring {@link CubeExploreQuery}.
     */
    public final class From {
      private From() {}

      /**
       * Sets resolution for {@link CubeExploreQuery}.
       * @param amount amount of units
       * @param timeUnit unit type
       * @return builder for configuring {@link CubeExploreQuery}
       */
      public Where resolution(long amount, TimeUnit timeUnit) {
        Builder.this.resolution = (int) timeUnit.convert(amount, TimeUnit.SECONDS);
        return new Where();
      }
    }

    /**
     * Builder for configuring {@link CubeExploreQuery}.
     */
    public final class Where {
      private Where() {}

      /**
       * @return builder for configuring {@link CubeExploreQuery}
       */
      public Dimension where() {
        return new Dimension();
      }
    }

    /**
     * Builder for configuring {@link CubeExploreQuery}.
     */
    public final class Dimension {
      private Dimension() {}

      /**
       * Adds dimension value to filter by.
       * @param name name of dimension
       * @param value value of dimension
       * @return builder for configuring {@link CubeExploreQuery}
       */
      public Dimension dimension(String name, String value) {
        Builder.this.dimensionValues.add(new DimensionValue(name, value));
        return this;
      }

      /**
       * Adds dimension values to filter by.
       * @param dimValues dimension name, dimension value pairs to filter by
       * @return builder for configuring {@link CubeExploreQuery}
       */
      public Dimension dimensions(List<DimensionValue> dimValues) {
        Builder.this.dimensionValues.addAll(dimValues);
        return this;
      }

      /**
       * Defines time range for {@link CubeExploreQuery}.
       * @param startTsInSec start time inclusive (epoch in seconds)
       * @param endTsInSec end time exclusive (epoch in seconds)
       * @return builder for configuring {@link CubeExploreQuery}
       */
      public Limit timeRange(long startTsInSec, long endTsInSec) {
        Builder.this.startTs = startTsInSec;
        Builder.this.endTs = endTsInSec;
        return new Limit();
      }
    }

    /**
     * Builder for configuring {@link CubeExploreQuery}.
     */
    public final class Limit {
      private Limit() {}

      /**
       * Sets a limit on returned data points per time series
       * @param limit limit value
       * @return builder for configuring {@link CubeExploreQuery}
       */
      public Build limit(int limit) {
        Builder.this.limit = limit;
        return new Build();
      }
    }

    /**
     * Builder for configuring {@link CubeExploreQuery}.
     */
    public final class Build {
      private Build() {}

      /**
       * @return {@link CubeExploreQuery}
       */
      public CubeExploreQuery build() {
        return Builder.this.build();
      }
    }
  }

}
