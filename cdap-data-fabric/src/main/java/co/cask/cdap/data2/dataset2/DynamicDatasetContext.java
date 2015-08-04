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
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of {@link DatasetContext} that allows to dynamically load datasets
 * into a started {@link TransactionContext}. Datasets acquired from this context are distinct from any
 * Datasets instantiated outside this class.
 */
public class DynamicDatasetContext implements DatasetContext {

  private final TransactionContext context;
  private final MetricsContext metricsContext;
  private final Set<String> allowedDatasets;
  private final DatasetFramework datasetFramework;
  private final ClassLoader classLoader;
  private final Map<String, String> runtimeArguments;
  private final Set<DatasetCacheKey> txnInProgressDatasets = Sets.newHashSet();
  private final Id.Namespace namespace;
  private final List<Id> owners;

  /**
   * Provides a {@link LoadingCache} for caching the dataset instance per thread.
   * This method returns {@code null} by default.
   *
   * @return a {@link LoadingCache} or {@code null} to turn off caching.
   */
  @Nullable
  protected LoadingCache<Long, Map<DatasetCacheKey, Dataset>> getDatasetsCache() {
    return null;
  }

  /**
   * Get the runtime arguments for a specific dataset. All runtime arguments are retained. Additional arguments
   * that are matching the current scope are included in runtime arguments with their scope extracted.
   * For example, when getting the runtime arguments for a dataset named 'myds', additional arguments that start
   * with 'dataset.myds.' and 'dataset.*.' are included in the runtime arguments with the prefix stripped from the
   * arguments.
   *
   * @param name the name of the dataset
   *
   * @return runtime arguments for the given dataset
   */
  protected Map<String, String> getRuntimeArguments(String name) {
    return RuntimeArguments.extractScope(Scope.DATASET, name, runtimeArguments);
  }

  /**
   * Create a dynamic dataset context that will get datasets and add them to the transaction context.
   *
   * @param namespace the {@link Id.Namespace} in which all datasets are instantiated
   * @param context the transaction context
   * @param metricsContext if non-null, this context is used as the context for dataset metrics,
   *                       with an additional tag for the dataset name.
   * @param datasetFramework the dataset framework for creating dataset instances
   * @param classLoader the classloader to use when creating dataset instances
   */
  public DynamicDatasetContext(Id.Namespace namespace,
                               TransactionContext context,
                               @Nullable MetricsContext metricsContext,
                               DatasetFramework datasetFramework,
                               ClassLoader classLoader) {
    this(namespace, context, metricsContext, datasetFramework,
         classLoader, ImmutableMap.<String, String>of(), null, null);
  }

  /**
   * Create a dynamic dataset context that will get datasets and add them to the transaction context.
   *
   * @param namespace the {@link Id.Namespace} in which all datasets are instantiated
   * @param context the transaction context
   * @param metricsContext if non-null, this context is used as the context for dataset metrics,
   *                       with an additional tag for the dataset name.
   * @param datasetFramework the dataset framework for creating dataset instances
   * @param classLoader the classloader to use when creating dataset instances
   * @param runtimeArguments all runtime arguments that are available to datasets in the context. Runtime arguments
   *                         are expected to be scoped so that arguments for one dataset do not override arguments
   * @param datasets the set of datasets that are allowed to be created. If null, any dataset can be created
   * @param owners the {@link Id}s which own this context
   */
  public DynamicDatasetContext(Id.Namespace namespace,
                               TransactionContext context,
                               @Nullable MetricsContext metricsContext,
                               DatasetFramework datasetFramework,
                               ClassLoader classLoader,
                               Map<String, String> runtimeArguments,
                               @Nullable Set<String> datasets,
                               @Nullable List<? extends Id> owners) {
    this.namespace = namespace;
    this.owners = owners == null ? ImmutableList.<Id>of() : ImmutableList.copyOf(owners);
    this.context = context;
    this.metricsContext = metricsContext;
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
    return getDataset(name, DatasetDefinition.NO_ARGUMENTS);
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

    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, name);

    // apply arguments on top of runtime arguments for the dataset
    Map<String, String> dsArguments = Maps.newHashMap();
    Map<String, String> runtimeArgs = getRuntimeArguments(name);
    if (runtimeArgs != null) {
      dsArguments.putAll(runtimeArgs);
    }
    if (arguments != null) {
      dsArguments.putAll(arguments);
    }

    DatasetCacheKey datasetCacheKey = new DatasetCacheKey(name, dsArguments);
    try {
      Dataset dataset;
      if (getDatasetsCache() != null) {
        Map<DatasetCacheKey, Dataset> threadLocalMap = getDatasetsCache().get(Thread.currentThread().getId());
        dataset = threadLocalMap.get(datasetCacheKey);
        if (dataset == null) {
          dataset = datasetFramework.getDataset(datasetInstanceId, dsArguments, classLoader, owners);
          if (dataset != null) {
            threadLocalMap.put(datasetCacheKey, dataset);
          }
        }
      } else {
        dataset = datasetFramework.getDataset(datasetInstanceId, dsArguments, classLoader, owners);
      }

      if (dataset == null) {
        throw new DatasetInstantiationException(String.format("Dataset '%s' does not exist", name));
      }

      // For every instance of a TransactionAware dataset acquired, add it to the
      // current transaction only if it hasn't been added previously.
      if (dataset instanceof TransactionAware && !txnInProgressDatasets.contains(datasetCacheKey)) {
        context.addTransactionAware((TransactionAware) dataset);
        txnInProgressDatasets.add(datasetCacheKey);
      }

      if (dataset instanceof MeteredDataset && metricsContext != null) {
        ((MeteredDataset) dataset).setMetricsCollector(getMetricsContext(metricsContext, name));
      }

      @SuppressWarnings("unchecked")
      T t = (T) dataset;
      return t;

    } catch (DatasetInstantiationException e) {
      throw e;
    } catch (Throwable t) {
      throw new DatasetInstantiationException(String.format("Could not instantiate dataset '%s'", name), t);
    }
  }

  private static MetricsContext getMetricsContext(MetricsContext metricsContext, String datasetName) {
    return metricsContext.childContext(Constants.Metrics.Tag.DATASET, datasetName);
  }

}

