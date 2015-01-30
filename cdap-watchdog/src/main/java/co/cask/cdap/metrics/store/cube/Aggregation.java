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

package co.cask.cdap.metrics.store.cube;

import java.util.List;

/**
 * Defines an aggregation in {@link co.cask.cdap.metrics.store.cube.DefaultCube}.
 * <p/>
 * Aggregation usually defines a list of tags, or dimensions that are extracted from the {@link CubeFact} when it is
 * added to the {@link Cube}. Configuring a {@link Cube} to compute an aggregation allows later querying for data based
 * on those tags the data is aggregated for. See also {@link CubeQuery}.
 */
public interface Aggregation {
  /**
   * @return list of tags (dimensions) to aggregate by
   */
  List<String> getTagNames();

  /**
   * Filters out {@link CubeFact}s which should not be added into this aggregation.
   * @param fact fact to test
   * @return true if fact should be added to the aggregation, false otherwise.
   */
  boolean accept(CubeFact fact);
}
