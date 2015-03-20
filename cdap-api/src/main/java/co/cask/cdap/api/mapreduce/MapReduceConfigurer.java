/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.mapreduce;

import co.cask.cdap.api.Resources;

import java.util.Map;

/**
 * Configurer for configuring {@link MapReduce}.
 */
public interface MapReduceConfigurer {

  /**
   * Sets the name of the {@link MapReduce}.
   */
  void setName(String name);

  /**
   * Sets the description of the {@link MapReduce}.
   */
  void setDescription(String description);

  /**
   * Sets a set of properties that will be available through the {@link MapReduceSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  void setProperties(Map<String, String> properties);

  /**
   * Specifies set of dataset names that are used by the {@link MapReduce}.
   * @deprecated datasets used in runtime need not be specified in {@link MapReduce#configure}
   */
  @Deprecated
  void useDatasets(Iterable<String> datasets);

  /**
   * Sets the name of the dataset used as input for the {@link MapReduce}.
   */
  void setInputDataset(String dataset);

  /**
   * Sets the name of the dataset used as output for the {@link MapReduce}.
   */
  void setOutputDataset(String dataset);

  /**
   * Sets the resources requirement for Mapper task of the {@link MapReduce}.
   */
  void setMapperResources(Resources resources);

  /**
   * Sets the resources requirement for Reducer task of the {@link MapReduce}.
   */
  void setReducerResources(Resources resources);
}
