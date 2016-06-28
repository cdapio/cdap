/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.metadata.lineage.AccessType;

import java.util.Map;

/**
 * Extends {@link DatasetContext} with extra methods that take {@link AccessType}.
 * TODO (CDAP-5363): This interface shouldn't be Spark specific.
 */
public interface SparkDatasetContext extends DatasetContext {

  /**
   * Get an instance of the specified dataset, with the specified access type.
   *
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param <T> the type of the dataset
   * @param accessType the accessType
   */
  <T extends Dataset> T getDataset(String name, Map<String, String> arguments,
                                   AccessType accessType) throws DatasetInstantiationException;

  /**
   * Get an instance of the specified dataset, with the specified access type.
   *
   * @param namespace the namespace of the dataset
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param <T> the type of the dataset
   * @param accessType the accessType
   */
  <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments,
                                   AccessType accessType) throws DatasetInstantiationException;

}
