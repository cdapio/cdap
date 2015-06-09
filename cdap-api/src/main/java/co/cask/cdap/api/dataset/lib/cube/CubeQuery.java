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
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Defines a query to perform on {@link Cube} data.
 * </p>
 * Another way to think about the query is to map it to the following statement::
 * <pre><code>
 * SELECT count('read.ops')                                         {@literal <<} measure name and aggregation function
 * FROM aggregation1.1min_resolution                                {@literal <<} aggregation and resolution
 * GROUP BY dataset,                                                {@literal <<} groupByDimensions
 * WHERE namespace='ns1' AND app='myApp' AND program='myFlow' AND   {@literal <<} dimensionValues
 *       ts>=1423370200 AND ts{@literal <}1423398198                           {@literal <<} startTs and endTs
 * LIMIT 100                                                        {@literal <<} limit
 *
 * </code>
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
  private final Map<String, AggregationFunction> measurements;
  private final Map<String, String> dimensionValues;
  private final List<String> groupByDimensions;
  private final Interpolator interpolator;

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
                   Map<String, AggregationFunction> measurements,
                   Map<String, String> dimensionValues, List<String> groupByDimensions,
                   @Nullable Interpolator interpolator) {
    this.aggregation = aggregation;
    this.startTs = startTs;
    this.endTs = endTs;
    this.resolution = resolution;
    this.limit = limit;
    this.measurements = measurements;
    this.dimensionValues = Collections.unmodifiableMap(new HashMap<>(dimensionValues));
    this.groupByDimensions = Collections.unmodifiableList(new ArrayList<>(groupByDimensions));
    this.interpolator = interpolator;
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

  public Map<String, AggregationFunction> getMeasurements() {
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

  /**
   * @return {@link Builder} to build {@link CubeQuery}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds {@link CubeQuery}.
   */
  public static final class Builder {
    private String aggregation;
    private long startTs;
    private long endTs;
    private int resolution;
    private int limit;
    private Map<String, AggregationFunction> measurements = new HashMap<>();
    private Map<String, String> dimensionValues = new HashMap<>();
    private List<String> groupByDimensions = new ArrayList<>();
    private Interpolator interpolator;

    /**
     * @return builder for configuring {@link CubeQuery}
     */
    public Select select() {
      return new Select();
    }

    /**
     * @return instance of {@link CubeQuery}
     */
    private CubeQuery build() {
      return new CubeQuery(aggregation, startTs, endTs, resolution, limit,
                           measurements, dimensionValues, groupByDimensions, interpolator);
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class Select {
      private Select() {}

      /**
       * Adds measurement to be included in selection of {@link CubeQuery}.
       * @param name name of the measurement
       * @param aggFunc function to be used if aggregation of measurement value is needed
       * @return builder for configuring {@link CubeQuery}
       */
      public Measurement measurement(String name, AggregationFunction aggFunc) {
        Builder.this.measurements.put(name, aggFunc);
        return new Measurement();
      }

      /**
       * Adds measurements to be included in selection of {@link CubeQuery}.
       * @param measurements map of measurement name, agg function to include
       * @return builder for configuring {@link CubeQuery}
       */
      public Measurement measurements(Map<String, AggregationFunction> measurements) {
        Builder.this.measurements.putAll(measurements);
        return new Measurement();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class Measurement {
      private Measurement() {}

      /**
       * Adds measurement to be included in selection of {@link CubeQuery}.
       * @param name name of the measurement
       * @param aggFunc function to be used if aggregation of measurement value is needed
       * @return builder for configuring {@link CubeQuery}
       */
      public Measurement measurement(String name, AggregationFunction aggFunc) {
        Builder.this.measurements.put(name, aggFunc);
        return this;
      }

      /**
       * Adds measurements to be included in selection of {@link CubeQuery}.
       * @param measurements map of measurement name, agg function to include
       * @return builder for configuring {@link CubeQuery}
       */
      public Measurement measurements(Map<String, AggregationFunction> measurements) {
        Builder.this.measurements.putAll(measurements);
        return new Measurement();
      }

      /**
       * Defines aggregation view to query from.
       * @param aggregation name of the aggregation view
       * @return builder for configuring {@link CubeQuery}
       */
      public From from(String aggregation) {
        Builder.this.aggregation = aggregation;
        return new From();
      }

      /**
       * Sets aggregation view to query from to be auto-selected based on other parameters of the query.
       * @return builder for configuring {@link CubeQuery}
       */
      public From from() {
        Builder.this.aggregation = null;
        return new From();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class From {
      private From() {}

      /**
       * Sets resolution for {@link CubeQuery}.
       * @param amount amount of units
       * @param timeUnit unit type
       * @return builder for configuring {@link CubeQuery}
       */
      public Where resolution(long amount, TimeUnit timeUnit) {
        Builder.this.resolution = (int) timeUnit.convert(amount, TimeUnit.SECONDS);
        return new Where();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class Where {
      private Where() {}

      /**
       * @return builder for configuring {@link CubeQuery}
       */
      public Dimension where() {
        return new Dimension();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class Dimension {
      private Dimension() {}

      /**
       * Adds dimension value to filter by.
       * @param name name of dimension
       * @param value value of dimension
       * @return builder for configuring {@link CubeQuery}
       */
      public Dimension dimension(String name, String value) {
        Builder.this.dimensionValues.put(name, value);
        return this;
      }

      /**
       * Adds dimension values to filter by.
       * @param dimValues dimension name, dimension value pairs to filter by
       * @return builder for configuring {@link CubeQuery}
       */
      public Dimension dimensions(Map<String, String> dimValues) {
        Builder.this.dimensionValues.putAll(dimValues);
        return this;
      }

      /**
       * Defines time range for {@link CubeQuery}.
       * @param startTsInSec start time inclusive (epoch in seconds)
       * @param endTsInSec end time exclusive (epoch in seconds)
       * @return builder for configuring {@link CubeQuery}
       */
      public GroupBy timeRange(long startTsInSec, long endTsInSec) {
        Builder.this.startTs = startTsInSec;
        Builder.this.endTs = endTsInSec;
        return new GroupBy();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class GroupBy {
      private GroupBy() {}

      /**
       * @return builder for configuring {@link CubeQuery}
       */
      public GroupByDimension groupBy() {
        return new GroupByDimension();
      }

      /**
       * Sets a limit on returned data points per time series
       * @param limit limit value
       * @return builder for configuring {@link CubeQuery}
       */
      public Limit limit(int limit) {
        Builder.this.limit = limit;
        return new Limit();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class GroupByDimension {
      private GroupByDimension() {}

      /**
       * Adds dimension to use for grouping results into time series.
       * @param name name of the dimension
       * @return builder for configuring {@link CubeQuery}
       */
      public GroupByDimension dimension(String name) {
        Builder.this.groupByDimensions.add(name);
        return this;
      }

      /**
       * Adds dimensions to use for grouping results into time series.
       * @param names names of the dimensions
       * @return builder for configuring {@link CubeQuery}
       */
      public GroupByDimension dimensions(List<String> names) {
        Builder.this.groupByDimensions.addAll(names);
        return this;
      }

      /**
       * Sets a limit on returned data points per time series
       * @param limit limit value
       * @return builder for configuring {@link CubeQuery}
       */
      public Limit limit(int limit) {
        Builder.this.limit = limit;
        return new Limit();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class Limit {
      private Limit() {}

      /**
       * Sets {@link Interpolator} to use for {@link CubeQuery}.
       * @param interpolator interpolator to use
       * @return builder for configuring {@link CubeQuery}
       */
      public Build interpolator(Interpolator interpolator) {
        Builder.this.interpolator = interpolator;
        return new Build();
      }

      /**
       * @return {@link CubeQuery}
       */
      public CubeQuery build() {
        return Builder.this.build();
      }
    }

    /**
     * Builder for configuring {@link CubeQuery}.
     */
    public final class Build {
      private Build() {}

      /**
       * @return {@link CubeQuery}
       */
      public CubeQuery build() {
        return Builder.this.build();
      }
    }
  }

}
