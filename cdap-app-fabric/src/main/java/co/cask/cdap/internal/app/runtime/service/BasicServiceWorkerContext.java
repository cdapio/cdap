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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.data.DataSetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.ServiceWorkerContext;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.app.metrics.ServiceRunnableMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link ServiceWorkerContext}.
 */
public class BasicServiceWorkerContext extends AbstractContext implements ServiceWorkerContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicServiceWorkerContext.class);
  private final Map<String, String> runtimeArgs;
  private final Set<String> datasets;
  private final TransactionSystemClient transactionSystemClient;
  private final DatasetFramework datasetFramework;
  private final ClassLoader programClassLoader;
  private final ServiceRunnableMetrics serviceRunnableMetrics;
  private final int instanceId;
  private final int instanceCount;

  /**
   * Create a ServiceWorkerContext with runtime arguments and access to Datasets.
   * @param cConfiguration configuration used by the datasetFramework.
   * @param runtimeArgs of the worker.
   * @param datasets the worker is allowed to access.
   * @param datasetFramework used to get datasets.
   * @param transactionSystemClient used to transactionalize operations.
   */
  public BasicServiceWorkerContext(Program program, RunId runId, int instanceId, int instanceCount,
                                   String runnableName, ClassLoader programClassLoader, CConfiguration cConfiguration,
                                   Map<String, String> runtimeArgs, Set<String> datasets,
                                   MetricsCollectionService metricsCollectionService,
                                   DatasetFramework datasetFramework,
                                   TransactionSystemClient transactionSystemClient,
                                   DiscoveryServiceClient discoveryServiceClient) {
    super(program, runId, datasets, getMetricContext(program, runnableName, instanceId), metricsCollectionService,
          datasetFramework, cConfiguration, discoveryServiceClient);
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.programClassLoader = programClassLoader;
    this.runtimeArgs = ImmutableMap.copyOf(runtimeArgs);
    this.datasets = ImmutableSet.copyOf(datasets);
    this.transactionSystemClient = transactionSystemClient;
    this.datasetFramework = new NamespacedDatasetFramework(datasetFramework,
                                                           new DefaultDatasetNamespace(cConfiguration, Namespace.USER));
    this.serviceRunnableMetrics = new ServiceRunnableMetrics(metricsCollectionService,
                                                             getMetricContext(program, runnableName, instanceId));

  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }

  @Override
  public Metrics getMetrics() {
    return serviceRunnableMetrics;
  }

  private static String getMetricContext(Program program, String runnableName, int instanceId) {
    return String.format("%s.%s.%s.%s.%s", program.getApplicationId(), TypeId.getMetricContextId(ProgramType.SERVICE),
                         program.getName(), runnableName, instanceId);
  }

  @Override
  public void execute(TxRunnable runnable) {
    final TransactionContext context = new TransactionContext(transactionSystemClient);
    try {
      context.start();
      runnable.run(new ServiceWorkerDatasetContext(context));
      context.finish();
    } catch (TransactionFailureException e) {
      abortTransaction(e, "Failed to commit. Aborting transaction.", context);
    } catch (Exception e) {
      abortTransaction(e, "Exception occurred running user code. Aborting transaction.", context);
    }
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  private void abortTransaction(Exception e, String message, TransactionContext context) {
    try {
      LOG.error(message);
      context.abort();
      throw Throwables.propagate(e);
    } catch (TransactionFailureException e1) {
      LOG.error("Failed to abort transaction.");
      throw Throwables.propagate(e1);
    }
  }

  private class ServiceWorkerDatasetContext implements DataSetContext {
    private final TransactionContext context;

    private ServiceWorkerDatasetContext(TransactionContext context) {
      this.context = context;
    }

    @Override
    public <T extends Closeable> T getDataSet(String name) throws DataSetInstantiationException {
      return getDataSet(name, DatasetDefinition.NO_ARGUMENTS);
    }

    @Override
    public <T extends Closeable> T getDataSet(String name, Map<String, String> arguments)
      throws DataSetInstantiationException {
      String datasetNotUsedError = String.format("Trying to access dataset %s that is not declared as used " +
                                                   "by the Worker. Specificy datasets used using useDataset() " +
                                                   "method in the Workers's configure.", name);
      Preconditions.checkArgument(datasets.contains(name), datasetNotUsedError);

      try {
        Dataset dataset = datasetFramework.getDataset(name, arguments,
                                                      programClassLoader);
        if (dataset == null) {
          throw new DataSetInstantiationException(String.format("Dataset %s does not exist.", name));
        }
        context.addTransactionAware((TransactionAware) dataset);
        return (T) dataset;
      } catch (DatasetManagementException e) {
        LOG.error("Could not get dataset metainfo.");
        throw Throwables.propagate(e);
      } catch (IOException e) {
        LOG.error("Could not instantiate dataset.");
        throw Throwables.propagate(e);
      }
    }
  }
}
