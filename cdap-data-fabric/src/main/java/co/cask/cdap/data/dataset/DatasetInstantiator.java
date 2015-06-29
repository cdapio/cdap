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

package co.cask.cdap.data.dataset;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The data set instantiator creates instances of data sets at runtime.
 */
public class DatasetInstantiator implements DatasetContext {

  private final DatasetFramework datasetFramework;
  // the class loader to use for data set classes
  private final ClassLoader classLoader;
  private final Set<TransactionAware> txAware = Sets.newIdentityHashSet();
  // in this collection we have only datasets initialized with getDataset() which is OK for now...
  private final Map<TransactionAware, String> txAwareToMetricNames = Maps.newIdentityHashMap();

  private final MetricsContext metricsContext;
  private final Id.Namespace namespace;
  private final Iterable<? extends Id> owners;

  /**
   * Constructor from data fabric.
   *
   * @param namespace the {@link Id.Namespace} in which this dataset is used
   * @param owners the {@link Id} which is using this dataset
   * @param classLoader the class loader to use for loading dataset classes.
   *                    If null, then the default class loader is used
   */
  public DatasetInstantiator(Id.Namespace namespace,
                             DatasetFramework datasetFramework,
                             ClassLoader classLoader,
                             @Nullable Iterable<? extends Id> owners,
                             @Nullable MetricsContext metricsContext) {
    this.namespace = namespace;
    this.owners = owners;
    this.classLoader = classLoader;
    this.metricsContext = metricsContext;
    this.datasetFramework = datasetFramework;
  }

  @Override
  public <T extends Dataset> T getDataset(String dataSetName)
    throws DatasetInstantiationException {
    return getDataset(dataSetName, DatasetDefinition.NO_ARGUMENTS);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {

    T dataset;
    try {
      if (!datasetFramework.hasInstance(Id.DatasetInstance.from(namespace, name))) {
        throw new DatasetInstantiationException("Trying to access dataset that does not exist: " + name);
      }

      dataset = datasetFramework.getDataset(Id.DatasetInstance.from(namespace, name),
                                            arguments, classLoader, owners);
      if (dataset == null) {
        throw new DatasetInstantiationException("Failed to access dataset: " + name);
      }

    } catch (Exception e) {
      throw new DatasetInstantiationException("Failed to access dataset: " + name, e);
    }

    if (dataset instanceof TransactionAware) {
      txAware.add((TransactionAware) dataset);
      txAwareToMetricNames.put((TransactionAware) dataset, name);
    }

    if (dataset instanceof MeteredDataset && metricsContext != null) {
      ((MeteredDataset) dataset).setMetricsCollector(getMetricsContext(metricsContext, name));
    }

    return dataset;
  }

  /**
   * Returns an immutable life Iterable of {@link TransactionAware} objects.
   */
  public Iterable<TransactionAware> getTransactionAware() {
    return Iterables.unmodifiableIterable(txAware);
  }

  public void addTransactionAware(TransactionAware transactionAware) {
    txAware.add(transactionAware);
  }

  public void removeTransactionAware(TransactionAware transactionAware) {
    txAware.remove(transactionAware);
  }

  private static MetricsContext getMetricsContext(MetricsContext metricsContext, String datasetName) {
    return metricsContext.childContext(Constants.Metrics.Tag.DATASET, datasetName);
  }
}
