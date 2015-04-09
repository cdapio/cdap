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
import co.cask.cdap.api.dataset.lib.cube.TagValue;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.timeseries.EntityTable;
import co.cask.cdap.data2.dataset2.lib.timeseries.FactTable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Implementation of {@link Cube} as a {@link Dataset} on top of {@link Table}s.
 */
public class CubeDataset extends AbstractDataset implements Cube {
  private final Map<Integer, Table> resolutionTables;
  private final Table entityTable;
  private final DefaultCube cube;

  public CubeDataset(String name, Table entityTable,
                     Map<Integer, Table> resolutionTables,
                     Collection<Aggregation> aggregations) {
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
                                aggregations);
  }

  @Override
  public void add(CubeFact fact) throws Exception {
    cube.add(fact);
  }

  @Override
  public void add(Collection<? extends CubeFact> facts) throws Exception {
    cube.add(facts);
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery query) throws Exception {
    return cube.query(query);
  }

  @Override
  public void delete(CubeDeleteQuery query) throws Exception {
    cube.delete(query);
  }

  @Override
  public Collection<TagValue> findNextAvailableTags(CubeExploreQuery query) throws Exception {
    return cube.findNextAvailableTags(query);
  }

  @Override
  public Collection<String> findMeasureNames(CubeExploreQuery query) throws Exception {
    return cube.findMeasureNames(query);
  }

  @Override
  public void close() throws IOException {
    entityTable.close();
    for (Table table : resolutionTables.values()) {
      table.close();
    }
  }

  private static final class FactTableSupplierImpl implements FactTableSupplier {
    private final Table entityTable;
    private final Map<Integer, Table> resolutionTables;

    private FactTableSupplierImpl(Table entityTable, Map<Integer, Table> resolutionTables) {
      this.entityTable = entityTable;
      this.resolutionTables = resolutionTables;
    }

    @Override
    public FactTable get(int resolution, int rollTime) {
      return new FactTable(new MetricsTableOnTable(resolutionTables.get(resolution)),
                           new EntityTable(new MetricsTableOnTable(entityTable)),
                           resolution, rollTime);
    }
  }
}
