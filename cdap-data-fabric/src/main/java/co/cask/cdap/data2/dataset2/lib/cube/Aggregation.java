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

import co.cask.cdap.api.dataset.lib.cube.CubeFact;

import java.util.List;

/**
 * Defines an aggregation in {@link DefaultCube}.
 * <p/>
 * Aggregation usually defines a list of dimensions, or dimensions that are extracted from the
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact} when it is
 * added to the {@link co.cask.cdap.api.dataset.lib.cube.Cube}. Configuring a
 * {@link co.cask.cdap.api.dataset.lib.cube.Cube} to compute an aggregation allows later querying for data based
 * on those dimensions the data is aggregated for. See also {@link co.cask.cdap.api.dataset.lib.cube.CubeQuery}.
 */
public interface Aggregation {
  /**
   * Defines the dimensions to do the aggregation for.
   * <p/>
   * Depending on the implementation of a {@link co.cask.cdap.api.dataset.lib.cube.Cube},
   * the order of the dimension names can be used as is to form a row key.
   * That in turn may affect the performance of the querying. Usually you want to put most frequently defined dimensions
   * (dimensions with values to slice by in {@link co.cask.cdap.api.dataset.lib.cube.CubeQuery} to be in front.
   *
   * @return list of dimensions (dimensions) to aggregate by
   */
  List<String> getDimensionNames();

  /**
   * Filters out {@link co.cask.cdap.api.dataset.lib.cube.CubeFact}s which should not be added into this aggregation.
   * @param fact fact to test
   * @return true if fact should be added to the aggregation, false otherwise.
   */
  boolean accept(CubeFact fact);
}
