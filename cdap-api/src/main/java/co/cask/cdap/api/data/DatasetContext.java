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
 * This interface provides methods that instantiate or dismiss a Dataset during the runtime
 * of a program. If the same arguments are provided for the same dataset, then the
 * returned instance is the same as returned by previous calls.
 */
public interface DatasetContext {

  /**
   * Get an instance of the specified Dataset.
   *
   * @param name The name of the Dataset
   * @param <T> The type of the Dataset
   * @return An instance of the specified Dataset, never null.
   * @throws DatasetInstantiationException If the Dataset cannot be instantiated: its class
   *         cannot be loaded; the default constructor throws an exception; or the Dataset
   *         cannot be opened (for example, one of the underlying tables in the DataFabric
   *         cannot be accessed).
   */
  <T extends Dataset> T getDataset(String name)
    throws DatasetInstantiationException;

  /**
   * Get an instance of the specified Dataset.
   *
   * @param name The name of the Dataset
   * @param arguments the arguments for this dataset instance
   * @param <T> The type of the Dataset
   * @return An instance of the specified Dataset, never null.
   * @throws DatasetInstantiationException If the Dataset cannot be instantiated: its class
   *         cannot be loaded; the default constructor throws an exception; or the Dataset
   *         cannot be opened (for example, one of the underlying tables in the DataFabric
   *         cannot be accessed).
   */
  <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException;

  /**
   * Calling this means that the dataset is not used by the caller any more,
   * and the DatasetContext is free to dismiss or reuse it. The dataset must
   * have been acquired through this context using {@link #getDataset(String)}
   * or {@link #getDataset(String, Map)}. It is up to the implementation of the
   * context whether this dataset is discarded, or whether it is reused as the
   * result for subsequent {@link #getDataset} calls with the same arguments.
   * This may be influenced by resource constraints, expiration policy, or
   * advanced configuration.
   *
   * @param dataset The dataset to be released.
   */
  @Beta
  void releaseDataset(Dataset dataset);

  /**
   * Calling this means that the dataset is not used by the caller any more,
   * and the DatasetContext must close and discard it as soon as possible.
   * The dataset must have been acquired through this context using
   * {@link #getDataset(String)} or {@link #getDataset(String, Map)}.
   * <p>
   * It is guaranteed that no subsequent
   * invocation of {@link #getDataset} will return the same object. The only
   * exception is if getDataset() is called again before the dataset could be
   * effectively discarded. For example, if the dataset participates in a
   * transaction, then the system can only discard it after that transaction
   * has completed. If getDataset() is called again during the same transaction,
   * then the DatasetContext will return the same object, and this effectively
   * cancels the discardDataset(), because the dataset was reacquired before it
   * could be discarded.
   * </p>
   *
   * @param dataset The dataset to be dismissed.
   */
  @Beta
  void discardDataset(Dataset dataset);
}
