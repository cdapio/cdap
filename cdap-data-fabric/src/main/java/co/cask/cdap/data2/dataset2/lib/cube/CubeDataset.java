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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.timeseries.EntityTable;
import co.cask.cdap.data2.dataset2.lib.timeseries.FactTable;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Implementation of {@link Cube} as a {@link Dataset} on top of {@link Table}s.
 */
public class CubeDataset extends AbstractDataset implements Cube {
  private final Map<Integer, Table> resolutionTables;
  private final MetricsTable entityTable;
  private final DefaultCube cube;

  // NOTE: entityTable has to be a non-transactional MetricsTable: DefaultCube internally uses in-memory caching for
  //       data stored in it and requires the table writes to be durable independent on transaction commit success.
  public CubeDataset(String name, MetricsTable entityTable,
                     Map<Integer, Table> resolutionTables,
                     Map<String, ? extends Aggregation> aggregations) {
    super(name, entityTable, resolutionTables.values().toArray(new Dataset[resolutionTables.values().size()]));
    this.entityTable = entityTable;
    this.resolutionTables = resolutionTables;
    int[] resolutions = new int[resolutionTables.keySet().size()];
    int index = 0;
    for (Integer resolution : resolutionTables.keySet()) {
      resolutions[index++] = resolution;
    }
    this.cube = new DefaultCube(resolutions,
                                new FactTableSupplierImpl(entityTable, resolutionTables),
                                aggregations, ImmutableMap.<String, AggregationAlias>of());
  }

  @Override
  public void add(CubeFact fact) {
    cube.add(fact);
  }

  @Override
  public void add(Collection<? extends CubeFact> facts) {
    cube.add(facts);
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery query) {
    return cube.query(query);
  }

  @Override
  public void delete(CubeDeleteQuery query) {
    cube.delete(query);
  }

  @Override
  public Collection<DimensionValue> findDimensionValues(CubeExploreQuery query) {
    return cube.findDimensionValues(query);
  }

  @Override
  public Collection<String> findMeasureNames(CubeExploreQuery query) {
    return cube.findMeasureNames(query);
  }

  @Override
  public void write(Object ignored, CubeFact cubeFact) {
    add(cubeFact);
  }

  @Override
  public void close() throws IOException {
    entityTable.close();
    for (Table table : resolutionTables.values()) {
      table.close();
    }
  }

  private static final class FactTableSupplierImpl implements FactTableSupplier {
    private final MetricsTable entityTable;
    private final Map<Integer, Table> resolutionTables;

    private FactTableSupplierImpl(MetricsTable entityTable, Map<Integer, Table> resolutionTables) {
      this.entityTable = entityTable;
      this.resolutionTables = resolutionTables;
    }

    @Override
    public FactTable get(int resolution, int rollTime) {
      return new FactTable(new MetricsTableOnTable(resolutionTables.get(resolution)),
                           new EntityTable(entityTable),
                           resolution, rollTime);
    }
  }
}
