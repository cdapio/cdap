/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of {@link co.cask.cdap.api.data.DatasetContext} that allows to dynamically load datasets
 * into a started {@link TransactionContext}.
 */
public abstract class DynamicDatasetContext implements DatasetContext {
  private final TransactionContext context;
  private final Set<String> allowedDatasets;
  private final DatasetFramework datasetFramework;
  private final ClassLoader classLoader;
  private final Map<String, String> runtimeArguments;

  @Nullable
  protected abstract LoadingCache<Long, Map<String, Dataset>> getDatasetsCache();

  /**
   * Get the runtime arguments for a specific dataset. Arguments not in the scope of the dataset are filtered out.
   * For example, when getting the runtime arguments for a dataset named 'myds', only runtime arguments that start
   * with 'dataset.myds.' are returned, with the prefix stripped from the arguments.
   *
   * @param name the name of the dataset
   * @return runtime arguments for the given dataset
   */
  protected Map<String, String> getRuntimeArguments(String name) {
    return RuntimeArguments.extractScope(Scope.DATASET, name, runtimeArguments);
  }

  public DynamicDatasetContext(TransactionContext context, DatasetFramework datasetFramework, ClassLoader classLoader) {
    this(context, datasetFramework, classLoader, null, ImmutableMap.<String, String>of());
  }

  /**
   * Create a dynamic dataset context that will get datasets and add them to the transaction context.
   *
   * @param context the transaction context
   * @param datasetFramework the dataset framework for creating dataset instances
   * @param classLoader the classloader to use when creating dataset instances
   * @param datasets the set of datasets that are allowed to be created. If null, any dataset can be created
   * @param runtimeArguments all runtime arguments that are available to datasets in the context. Runtime arguments
   *                         are expected to be scoped so that arguments for one dataset do not override arguments
   *                         for another. For example, dataset.myfileset.output.path instead of output.path
   */
  public DynamicDatasetContext(TransactionContext context, DatasetFramework datasetFramework, ClassLoader classLoader,
                               @Nullable Set<String> datasets, Map<String, String> runtimeArguments) {
    this.context = context;
    this.allowedDatasets = datasets == null ? null : ImmutableSet.copyOf(datasets);
    this.datasetFramework = datasetFramework;
    this.classLoader = classLoader;
    this.runtimeArguments = ImmutableMap.copyOf(runtimeArguments);
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
    return getDataset(name, getRuntimeArguments(name));
  }

  /**
   * Get an instance of the specified Dataset. This method is thread-safe and may be used concurrently.
   * The returned dataset is also added to the transaction of the current {@link TransactionContext} call.
   * Arguments given here will be applied on top of the runtime arguments for the dataset.
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

    Map<String, String> dsArguments = getRuntimeArguments(name);
    dsArguments.putAll(arguments);

    try {
      Dataset dataset;
      if (getDatasetsCache() != null) {
        Map<String, Dataset> threadLocalMap = getDatasetsCache().get(Thread.currentThread().getId());
        dataset = threadLocalMap.get(name);
        if (dataset == null) {
          dataset = datasetFramework.getDataset(name, dsArguments, classLoader);
          if (dataset != null) {
            threadLocalMap.put(name, dataset);
          }
        }
      } else {
        dataset = datasetFramework.getDataset(name, dsArguments, classLoader);
      }

      if (dataset == null) {
        throw new DatasetInstantiationException(String.format("Dataset '%s' does not exist", name));
      }

      if (dataset instanceof TransactionAware) {
        context.addTransactionAware((TransactionAware) dataset);
      }

      return (T) dataset;
    } catch (DatasetInstantiationException e) {
      throw e;
    } catch (Throwable t) {
      throw new DatasetInstantiationException(String.format("Could not instantiate dataset '%s'", name), t);
    }
  }
}

