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

package co.cask.cdap.data.dataset;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The data set instantiator creates instances of data sets at runtime. It
 * must be called from the execution context to get operational instances
 * of data sets. Given a list of data set specs and a data fabric runtime it
 * can construct an instance of a data set and inject the data fabric runtime
 * into its base tables (and other built-in data sets).
 *
 * The instantiation and injection uses Java reflection a lot. This may look
 * unclean, but it helps us keep the DataSet API clean and simple: no need
 * to pass in data fabric runtime; no exposure of developers to the raw
 * data fabric; and developers only interact with Datasets.
 */
public class DatasetInstantiator implements DatasetContext {

  private final DatasetFramework datasetFramework;
  // the class loader to use for data set classes
  private final ClassLoader classLoader;
  private final Set<TransactionAware> txAware = Sets.newIdentityHashSet();
  // in this collection we have only datasets initialized with getDataset() which is OK for now...
  private final Map<TransactionAware, String> txAwareToMetricNames = Maps.newIdentityHashMap();

  private final MetricsCollector metricsCollector;
  private final Id.Namespace namespaceId;

  /**
   * Constructor from data fabric.
   *
   * @param namespaceId the {@link Id.Namespace} in which this dataset exists
   * @param classLoader the class loader to use for loading data set classes.
   *                    If null, then the default class loader is used
   */
  public DatasetInstantiator(Id.Namespace namespaceId,
                             DatasetFramework datasetFramework,
                             CConfiguration configuration,
                             ClassLoader classLoader,
                             @Nullable
                             MetricsCollector metricsCollector) {
    this.namespaceId = namespaceId;
    this.classLoader = classLoader;
    this.metricsCollector = metricsCollector;
    // todo: should be passed in already namespaced. Refactor
    this.datasetFramework =
      new NamespacedDatasetFramework(datasetFramework,
                                     new DefaultDatasetNamespace(configuration, Namespace.USER));
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
      if (!datasetFramework.hasInstance(Id.DatasetInstance.from(namespaceId, name))) {
        throw new DatasetInstantiationException("Trying to access dataset that does not exist: " + name);
      }

      dataset = datasetFramework.getDataset(Id.DatasetInstance.from(namespaceId, name), arguments, classLoader);
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

    if (dataset instanceof MeteredDataset) {
      ((MeteredDataset) dataset).setMetricsCollector(new MetricsCollectorImpl(name, metricsCollector));
    }

    return dataset;
  }

  /**
   * Returns an immutable life Iterable of {@link co.cask.tephra.TransactionAware} objects.
   */
  // NOTE: this is needed for now to minimize destruction of early integration of txds2
  public Iterable<TransactionAware> getTransactionAware() {
    return Iterables.unmodifiableIterable(txAware);
  }

  public void addTransactionAware(TransactionAware transactionAware) {
    txAware.add(transactionAware);
  }

  public void removeTransactionAware(TransactionAware transactionAware) {
    txAware.remove(transactionAware);
  }

  private static final class MetricsCollectorImpl implements MeteredDataset.MetricsCollector {
    private final MetricsCollector metricsCollector;

    private MetricsCollectorImpl(String datasetName,
                                 @Nullable
                                 MetricsCollector metricsCollector) {
      this.metricsCollector = metricsCollector == null ? null :
        metricsCollector.childCollector(Constants.Metrics.Tag.DATASET, datasetName);
    }

    @Override
    public void recordRead(int opsCount, int dataSize) {
      if (metricsCollector != null) {
        // todo: here we report duplicate metrics - need to change UI/docs and report once
        metricsCollector.increment("store.reads", 1);
        metricsCollector.increment("store.ops", 1);
        metricsCollector.increment("dataset.store.reads", 1);
        metricsCollector.increment("dataset.store.ops", 1);
      }
    }

    @Override
    public void recordWrite(int opsCount, int dataSize) {
      // todo: here we report duplicate metrics - need to change UI/docs and report once
      if (metricsCollector != null) {
        metricsCollector.increment("store.writes", 1);
        metricsCollector.increment("store.bytes", dataSize);
        metricsCollector.increment("store.ops", 1);
        metricsCollector.increment("dataset.store.writes", 1);
        metricsCollector.increment("dataset.store.bytes", dataSize);
        metricsCollector.increment("dataset.store.ops", 1);
      }
    }
  }
}
