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

package co.cask.cdap.api.data;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.Dataset;

import java.util.Map;

/**
 * A method that instantiates a Dataset at runtime.
 */
public interface DatasetContext {

  /**
   * Get an instance of the specified Dataset.
   *
   * @param name The name of the Dataset
   * @param <T> The type of the Dataset
   * @return A new instance of the specified Dataset, never null.
   * @throws DatasetInstantiationException If the Dataset cannot be instantiated: its class
   *         cannot be loaded; the default constructor throws an exception; or the Dataset
   *         cannot be opened (for example, one of the underlying tables in the DataFabric
   *         cannot be accessed).
   */
  public <T extends Dataset> T getDataset(String name)
    throws DatasetInstantiationException;

  /**
   * Get an instance of the specified Dataset.
   *
   * @param name The name of the Dataset
   * @param arguments the arguments for this dataset instance
   * @param <T> The type of the Dataset
   * @return A new instance of the specified Dataset, never null.
   * @throws DatasetInstantiationException If the Dataset cannot be instantiated: its class
   *         cannot be loaded; the default constructor throws an exception; or the Dataset
   *         cannot be opened (for example, one of the underlying tables in the DataFabric
   *         cannot be accessed).
   */
  @Beta
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException;

}
