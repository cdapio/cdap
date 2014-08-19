/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.data.DataSetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.tephra.TransactionAware;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
 * unclean, but it helps us keep the DataSet API clean and simple (no need
 * to pass in data fabric runtime, no exposure of the developer to the raw
 * data fabric, he only interacts with data sets).
 */
public class DataSetInstantiator implements DataSetContext {

  private static final Logger LOG = LoggerFactory.getLogger(DataSetInstantiator.class);
  private final DatasetFramework datasetFramework;
  // the class loader to use for data set classes
  private final ClassLoader classLoader;
  // the known data set specifications
  private final Map<String, DatasetCreationSpec> datasetsV2 = Maps.newHashMap();
  private final Set<TransactionAware> txAware = Sets.newIdentityHashSet();
  // in this collection we have only datasets initialized with getDataSet() which is OK for now...
  private final Map<TransactionAware, String> txAwareToMetricNames = Maps.newIdentityHashMap();

  /**
   * Constructor from data fabric.
   * @param classLoader the class loader to use for loading data set classes.
   *                    If null, then the default class loader is used
   */
  public DataSetInstantiator(DatasetFramework datasetFramework,
                             CConfiguration configuration, ClassLoader classLoader) {
    this.classLoader = classLoader;
    this.datasetFramework =
      new NamespacedDatasetFramework(datasetFramework,
                                     new DefaultDatasetNamespace(configuration, Namespace.USER));
  }

  @Override
  public <T extends Closeable> T getDataSet(String dataSetName)
    throws DataSetInstantiationException {
    return getDataSet(dataSetName, DatasetDefinition.NO_ARGUMENTS);
  }

  @Override
  public <T extends Closeable> T getDataSet(String name, Map<String, String> arguments)
    throws DataSetInstantiationException {
    return (T) getDataSet(name, arguments, this.datasetFramework);
  }

  public void setDataSets(Iterable<DatasetCreationSpec> creationSpec) {
    for (DatasetCreationSpec spec : creationSpec) {
      if (spec != null) {
        this.datasetsV2.put(spec.getInstanceName(), spec);
      }
    }
  }

  /**
   *  The main value of this class: Creates a new instance of a data set, as
   *  specified by the matching data set spec, and injects the data fabric
   *  runtime into the new data set.
   *  @param dataSetName the name of the data set to instantiate
   *  @param arguments the arguments for this dataset instance
   *  @throws co.cask.cdap.api.data.DataSetInstantiationException If failed to create the DataSet.
   */
  @SuppressWarnings("unchecked")
  public <T extends Closeable> T getDataSet(String dataSetName, Map<String, String> arguments,
                                            @Nullable DatasetFramework datasetFramework)
    throws DataSetInstantiationException {

    if (datasetFramework != null) {
      T dataSet = (T) getDataset(dataSetName, arguments, datasetFramework);
      if (dataSet != null) {
        return dataSet;
      }
    }

    throw logAndException(null, "No data set named %s can be instantiated.", dataSetName);
  }

  private <T extends Dataset> T getDataset(String datasetName, Map<String, String> arguments,
                                          DatasetFramework datasetFramework)
    throws DataSetInstantiationException {

    T dataset;
    try {
      if (!datasetFramework.hasInstance(datasetName)) {
        throw new DataSetInstantiationException("Trying to access dataset that does not exist: " + datasetName);
      }

      dataset = datasetFramework.getDataset(datasetName, arguments, classLoader);
      if (dataset == null) {
        throw new DataSetInstantiationException("Failed to access dataset: " + datasetName);
      }

    } catch (Exception e) {
      throw new DataSetInstantiationException("Failed to access dataset: " + datasetName, e);
    }

    if (dataset instanceof TransactionAware) {
      txAware.add((TransactionAware) dataset);
      txAwareToMetricNames.put((TransactionAware) dataset, datasetName);
    }

    return dataset;
  }

  /**
   * Returns an immutable life Iterable of {@link com.continuuity.tephra.TransactionAware} objects.
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

  /**
   * Helper method to log a message and create an exception. The caller is
   * responsible for throwing the exception.
   */
  private DataSetInstantiationException logAndException(Throwable e, String message, Object... params)
    throws DataSetInstantiationException {
    String msg;
    DataSetInstantiationException exn;
    if (e == null) {
      msg = String.format("Error instantiating data set: %s", String.format(message, params));
      exn = new DataSetInstantiationException(msg);
      LOG.error(msg);
    } else {
      msg = String.format("Error instantiating data set: %s. %s", String.format(message, params), e.getMessage());
      if (e instanceof DataSetInstantiationException) {
        exn = (DataSetInstantiationException) e;
      } else {
        exn = new DataSetInstantiationException(msg, e);
      }
      LOG.error(msg, e);
    }
    return exn;
  }

  public void setMetricsCollector(final MetricsCollectionService metricsCollectionService,
                                  final MetricsCollector programContextMetrics) {

    final MetricsCollector dataSetMetrics =
      metricsCollectionService.getCollector(MetricsScope.REACTOR, Constants.Metrics.DATASET_CONTEXT, "0");

    for (Map.Entry<TransactionAware, String> txAware : this.txAwareToMetricNames.entrySet()) {
      if (txAware.getKey() instanceof MeteredDataset) {
        // TODO: fix namespacing: we want to capture metrics of namespaced dataset name - see REACTOR-217
        final String dataSetName = txAware.getValue();
        MeteredDataset.MetricsCollector metricsCollector = new MeteredDataset.MetricsCollector() {
          @Override
          public void recordRead(int opsCount, int dataSize) {
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.reads", 1, dataSetName);
              programContextMetrics.gauge("store.ops", 1, dataSetName);
            }
            // these metrics are outside the context of any application and will stay unless explicitly
            // deleted.  Useful for dataset metrics that must survive the deletion of application metrics.
            if (dataSetMetrics != null) {
              dataSetMetrics.gauge("dataset.store.reads", 1, dataSetName);
              dataSetMetrics.gauge("dataset.store.ops", 1, dataSetName);
            }
          }

          @Override
          public void recordWrite(int opsCount, int dataSize) {
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.writes", 1, dataSetName);
              programContextMetrics.gauge("store.bytes", dataSize, dataSetName);
              programContextMetrics.gauge("store.ops", 1, dataSetName);
            }
            // these metrics are outside the context of any application and will stay unless explicitly
            // deleted.  Useful for dataset metrics that must survive the deletion of application metrics.
            if (dataSetMetrics != null) {
              dataSetMetrics.gauge("dataset.store.writes", 1, dataSetName);
              dataSetMetrics.gauge("dataset.store.bytes", dataSize, dataSetName);
              dataSetMetrics.gauge("dataset.store.ops", 1, dataSetName);
            }
          }
        };

        ((MeteredDataset) txAware.getKey()).setMetricsCollector(metricsCollector);
      }
    }
  }
}
