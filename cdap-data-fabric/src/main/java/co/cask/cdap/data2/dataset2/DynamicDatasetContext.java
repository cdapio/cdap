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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link co.cask.cdap.api.data.DatasetContext} that allows to dynamically load datasets
 * into a started {@link TransactionContext}.
 */
public abstract class DynamicDatasetContext implements DatasetContext {
  private final TransactionContext context;
  private final Set<String> allowedDatasets;
  private final DatasetFramework datasetFramework;
  private final ClassLoader classLoader;

  protected abstract LoadingCache<Long, Map<String, Dataset>> getDatasetsCache();

  public DynamicDatasetContext(TransactionContext context, DatasetFramework datasetFramework, ClassLoader classLoader) {
    this(context, datasetFramework, classLoader, null);
  }

  public DynamicDatasetContext(TransactionContext context, DatasetFramework datasetFramework, ClassLoader classLoader,
                               Set<String> datasets) {
    this.context = context;
    this.allowedDatasets = ImmutableSet.copyOf(datasets);
    this.datasetFramework = datasetFramework;
    this.classLoader = classLoader;
  }

  /**
   * Get an instance of the specified Dataset. This method is thread-safe and may be used concurrently.
   * The returned dataset is also added to the transaction of the current {@link TransactionContext}.
   *
   * @param name The name of the Dataset
   * @param <T> The type of the Dataset
   * @return A new instance of the specified Dataset, never null.
   * @throws DatasetInstantiationException If the Dataset cannot be instantiated: its class
   *         cannot be loaded; the default constructor throws an exception; or the Dataset
   *         cannot be opened (for example, one of the underlying tables in the DataFabric
   *         cannot be accessed).
   */
  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return getDataset(name, DatasetDefinition.NO_ARGUMENTS);
  }

  /**
   * Get an instance of the specified Dataset. This method is thread-safe and may be used concurrently.
   * The returned dataset is also added to the transaction of the current {@link TransactionContext} call.
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
  @Override
  public synchronized <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {

    if (allowedDatasets != null && !allowedDatasets.contains(name)) {
      throw new DatasetInstantiationException(
        String.format("Trying to access dataset '%s' that was not declared with " +
                        "useDataset() in the configure() method", name));
    }

    try {
      Dataset dataset;
      if (getDatasetsCache() != null) {
        Map<String, Dataset> threadLocalMap = getDatasetsCache().get(Thread.currentThread().getId());
        dataset = threadLocalMap.get(name);
        if (dataset == null) {
          dataset = datasetFramework.getDataset(name, arguments, classLoader);
          if (dataset != null) {
            threadLocalMap.put(name, dataset);
          }
        }
      } else {
        dataset = datasetFramework.getDataset(name, arguments, classLoader);
      }

      if (dataset == null) {
        throw new DatasetInstantiationException(String.format("Dataset '%s' does not exist", name));
      }

      if (dataset instanceof TransactionAware) {
        context.addTransactionAware((TransactionAware) dataset);
      }

      @SuppressWarnings("unchecked")
      T resultDataset = (T) dataset;
      return resultDataset;
    } catch (Throwable t) {
      throw new DatasetInstantiationException(String.format("Could not instantiate dataset '%s'", name), t);
    }
  }
}

