/*
 * Copyright 2015 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.cube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.lib.cube.CubeFact;
import io.cdap.cdap.api.dataset.lib.cube.CubeQuery;
import io.cdap.cdap.api.dataset.lib.cube.MeasureType;
import io.cdap.cdap.api.dataset.lib.cube.TimeSeries;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.data2.dataset2.lib.timeseries.EntityTable;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactTable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DefaultCube}.
 */
public class DefaultCubeTest extends AbstractCubeTest {

  @Test
  public void testGetTimeSeriesTableAcrossMeasuresAndAggFunctions() throws Exception {
    Aggregation agg = new DefaultAggregation(ImmutableList.of("region", "host"),
        ImmutableList.of("region"));
    Cube cube = getCube("timeSeriesTableCube", new int[]{1}, ImmutableMap.of("agg", agg));

    cube.add(ImmutableList.of(
        new CubeFact(10)
            .addDimensionValue("region", "us")
            .addDimensionValue("host", "h1")
            .addMeasurement("requests", MeasureType.COUNTER, 4)
            .addMeasurement("latency", MeasureType.GAUGE, 25),
        new CubeFact(10)
            .addDimensionValue("region", "us")
            .addDimensionValue("host", "h2")
            .addMeasurement("requests", MeasureType.COUNTER, 6)
            .addMeasurement("latency", MeasureType.GAUGE, 40),
        new CubeFact(10)
            .addDimensionValue("region", "us")
            .addMeasurement("requests", MeasureType.COUNTER, 99)
    ));

    CubeQuery query = CubeQuery.builder()
        .select()
        .measurement("requests", AggregationFunction.SUM)
        .measurement("latency", AggregationFunction.MAX)
        .from("agg")
        .resolution(1, TimeUnit.SECONDS)
        .where()
        .dimensions(Collections.singletonMap("region", "us"))
        .timeRange(0, 20)
        .groupBy()
        .dimension("host")
        .limit(100)
        .build();

    Collection<TimeSeries> series = cube.query(query);
    Map<String, Long> actualBySeries = new HashMap<>();
    for (TimeSeries ts : series) {
      String key = ts.getDimensionValues().get("host") + ":" + ts.getMeasureName();
      Assert.assertEquals(1, ts.getTimeValues().size());
      actualBySeries.put(key, ts.getTimeValues().get(0).getValue());
    }

    // Fact without 'host' dimension is skipped when grouping by 'host'; h1 and h2 populate 4 cells in the Table.
    Assert.assertEquals(4, actualBySeries.size());
    Assert.assertEquals(Long.valueOf(4L), actualBySeries.get("h1:requests"));
    Assert.assertEquals(Long.valueOf(25L), actualBySeries.get("h1:latency"));
    Assert.assertEquals(Long.valueOf(6L), actualBySeries.get("h2:requests"));
    Assert.assertEquals(Long.valueOf(40L), actualBySeries.get("h2:latency"));
  }

  @Override
  protected Cube getCube(String name, int[] resolutions, Map<String, ? extends Aggregation> aggregations)
    throws Exception {
    return getCube(name, resolutions, aggregations, 10, 1);
  }

  @Override
  protected Cube getCube(String name, int[] resolutions, Map<String, ? extends Aggregation> aggregations,
                         int coarseLagFactor, int coarseRoundFactor) throws Exception {

    FactTableSupplier supplier = (resolution, rollTime) -> {
      String entityTableName = "EntityTable-" + name;
      InMemoryTableService.create(entityTableName);
      String dataTableName = "DataTable-" + name + "-" + resolution;
      InMemoryTableService.create(dataTableName);
      return new FactTable(new InMemoryMetricsTable(dataTableName),
                           new EntityTable(new InMemoryMetricsTable(entityTableName)),
                           resolution, rollTime, coarseLagFactor, coarseRoundFactor);

    };

    return new DefaultCube(resolutions, supplier, aggregations, ImmutableMap.<String, AggregationAlias>of());
  }
}
