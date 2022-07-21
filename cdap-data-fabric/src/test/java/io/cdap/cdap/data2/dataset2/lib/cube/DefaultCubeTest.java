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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.data2.dataset2.lib.timeseries.EntityTable;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactTable;

import java.util.Map;

/**
 *
 */
public class DefaultCubeTest extends AbstractCubeTest {

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
