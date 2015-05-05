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

package co.cask.cdap.api.dataset.lib.cube;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.dataset.Dataset;

import java.util.Collection;
import java.util.List;

/**
 * Cube data set.
 * <p/>
 * Basic operations include adding {@link CubeFact}s and querying data.
 */
@Beta
public interface Cube extends Dataset, BatchWritable<Object, CubeFact> {
  /**
   * Property set to configure resolutions to aggregate for. Value is a comma-separated list of resolutions in seconds.
   */
  String PROPERTY_RESOLUTIONS = "dataset.cube.resolutions";

  /**
   * Adds {@link CubeFact} to this {@link Cube}.
   * @param fact fact to add.
   */
  void add(CubeFact fact);

  /**
   * Adds {@link CubeFact}s to this {@link Cube}.
   * @param facts facts to add.
   */
  void add(Collection<? extends CubeFact> facts);

  /**
   * Queries data in this {@link Cube}.
   * @param query query to perform.
   * @return {@link List} of {@link TimeSeries} that are result of the query.
   */
  Collection<TimeSeries> query(CubeQuery query);

  /**
   * Deletes the data specified by {@link CubeQuery} from all the fact tables.
   * @param query query specifies parameters for deletion.
   */
  void delete(CubeDeleteQuery query);

  /**
   * Finds dimension values, each of which is present in aggregated data selection defined with
   * {@link CubeExploreQuery}.
   * @param query query to perform
   * @return {@link Collection} of {@link DimensionValue} that are result of the query
   */
  Collection<DimensionValue> findDimensionValues(CubeExploreQuery query);

  /**
   * Queries data for available measureNames for the query specified by {@link CubeExploreQuery}
   * @param query query to perform
   * @return {@link Collection} of measureName string that are result of the query
   */
  Collection<String> findMeasureNames(CubeExploreQuery query);
}
