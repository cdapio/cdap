/*
 * Copyright © 2015 Cask Data, Inc.
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
 * This interface provides methods to instantiate and dismiss instances of Datasets
 * during the runtime of a program.
 */
public interface DatasetProvider extends DatasetContext {

  /**
   * Calling this means that the dataset is not used by the caller any more,
   * and the DatasetProvider is free to dismiss or reuse it. The dataset must
   * have been acquired through this provider using {@link #getDataset(String)}
   * or {@link #getDataset(String, Map)}. It is up to the implementation of the
   * provider whether this dataset is discarded, or whether it is reused as the
   * result for subsequent {@link #getDataset} calls with the same arguments.
   * This may be influenced by resource constraints, expiration policy, or
   * advanced configuration.
   * <p>
   * Note: In the current implementation, this is equivalent to {@link #discardDataset(Dataset)}.
   * </p>
   *
   * @param dataset The dataset to be released.
   */
  @Beta
  void releaseDataset(Dataset dataset);

  /**
   * Calling this means that the dataset is not used by the caller any more,
   * and the DatasetProvider must close and discard it as soon as possible.
   * The dataset must have been acquired through this provider using
   * {@link #getDataset(String)} or {@link #getDataset(String, Map)}.
   * <p>
   * It is guaranteed that no subsequent
   * invocation of {@link #getDataset} will return the same object. The only
   * exception is if getDataset() is called again before the dataset could be
   * effectively discarded. For example, if the dataset participates in a
   * transaction, then the system can only discard it after that transaction
   * has completed. If getDataset() is called again during the same transaction,
   * then the DatasetProvider will return the same object, and this effectively
   * cancels the discardDataset(), because the dataset was reacquired before it
   * could be discarded.
   * </p>
   *
   * @param dataset The dataset to be dismissed.
   */
  @Beta
  void discardDataset(Dataset dataset);
}
