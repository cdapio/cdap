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

import java.util.Collection;
import java.util.List;

/**
 * Cube data set.
 * <p/>
 * Basic operations include adding {@link CubeFact}s and querying data.
 */
public interface Cube {
  /**
   * Adds {@link CubeFact} to this {@link Cube}.
   * @param fact fact to add.
   */
  void add(CubeFact fact) throws Exception;

  /**
   * Queries data in this {@link Cube}.
   * @param query query to perform.
   * @return {@link List} of {@link TimeSeries} that are result of the query.
   */
  Collection<TimeSeries> query(CubeQuery query) throws Exception;

  /**
   * Deletes the data specified by {@link CubeQuery} from all the fact tables.
   * @param query query specifies parameters for deletion. groupByTags and resolution fields are not used.
   * @throws Exception
   */
  //todo: create separate DeleteCubeQuery class
  void delete(CubeQuery query) throws Exception;
}
