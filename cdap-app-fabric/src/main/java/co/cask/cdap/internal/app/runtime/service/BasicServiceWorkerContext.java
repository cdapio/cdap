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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.ServiceWorkerContext;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.app.metrics.ServiceRunnableMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Default implementation of {@link ServiceWorkerContext}.
 */
public class BasicServiceWorkerContext extends AbstractContext implements ServiceWorkerContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicServiceWorkerContext.class);

  private final ServiceWorkerSpecification specification;
  private final Set<String> datasets;
  private final TransactionSystemClient transactionSystemClient;
  private final DatasetFramework datasetFramework;
  private final ServiceRunnableMetrics serviceRunnableMetrics;
  private final int instanceId;
  private final int instanceCount;
  private final LoadingCache<Long, Map<String, Dataset>> datasetsCache;
  private final Program program;

  public BasicServiceWorkerContext(ServiceWorkerSpecification spec, Program program, RunId runId, int instanceId,
                                   int instanceCount, Arguments runtimeArgs, CConfiguration cConf,
                                   MetricsCollectionService metricsCollectionService,
                                   DatasetFramework datasetFramework,
                                   TransactionSystemClient transactionSystemClient,
                                   DiscoveryServiceClient discoveryServiceClient) {
    super(program, runId, runtimeArgs, spec.getDatasets(), getMetricContext(program, spec.getName(), instanceId),
          metricsCollectionService, datasetFramework, cConf, discoveryServiceClient);
    this.program = program;
    this.specification = spec;
    this.datasets = ImmutableSet.copyOf(spec.getDatasets());
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.transactionSystemClient = transactionSystemClient;
    this.datasetFramework = new NamespacedDatasetFramework(datasetFramework,
                                                           new DefaultDatasetNamespace(cConf, Namespace.USER));
    this.serviceRunnableMetrics = new ServiceRunnableMetrics(metricsCollectionService,
                                                             getMetricContext(program, spec.getName(), instanceId),
                                                             runId.getId());
    // A cache of datasets by threadId. Repeated requests for a dataset from the same thread returns the same
    // instance, thus avoiding the overhead of creating a new instance for every request.
    this.datasetsCache = CacheBuilder.newBuilder()
      .expireAfterAccess(2, TimeUnit.MINUTES)
      .removalListener(new RemovalListener<Long, Map<String, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<Long, Map<String, Dataset>> notification) {
          if (notification.getValue() != null) {
            for (Map.Entry<String, Dataset> entry : notification.getValue().entrySet()) {
              try {
                entry.getValue().close();
              } catch (IOException e) {
                LOG.error("Error closing dataset: {}", entry.getKey(), e);
              }
            }
          }
        }
      })
      .build(new CacheLoader<Long, Map<String, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public Map<String, Dataset> load(Long key) throws Exception {
          return Maps.newHashMap();
        }
      });
  }

  @Override
  public Metrics getMetrics() {
    return serviceRunnableMetrics;
  }

  public LoggingContext getLoggingContext() {
    return new UserServiceLoggingContext(program.getAccountId(), program.getApplicationId(),
                                         program.getId().getId(), specification.getName());
  }

  private static String getMetricContext(Program program, String runnableName, int instanceId) {
    return String.format("%s.%s.%s.%s.%d", program.getApplicationId(), TypeId.getMetricContextId(ProgramType.SERVICE),
                         program.getName(), runnableName, instanceId);
  }

  @Override
  public ServiceWorkerSpecification getSpecification() {
    return specification;
  }

  @Override
  public void execute(TxRunnable runnable) {
    final TransactionContext context = new TransactionContext(transactionSystemClient);
    try {
      context.start();
      runnable.run(new ServiceWorkerDatasetContext(context, datasetsCache));
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

  @Override
  public void close() {
    super.close();
    // Close all existing datasets that haven't been invalidated by the cache already.
    datasetsCache.invalidateAll();
    datasetsCache.cleanUp();
  }

  private void abortTransaction(Exception e, String message, TransactionContext context) {
    try {
      LOG.error(message, e);
      context.abort();
      throw Throwables.propagate(e);
    } catch (TransactionFailureException e1) {
      LOG.error("Failed to abort transaction.", e1);
      throw Throwables.propagate(e1);
    }
  }

  private class ServiceWorkerDatasetContext implements DatasetContext {
    private final TransactionContext context;
    private final LoadingCache<Long, Map<String, Dataset>> datasetsCache;

    private ServiceWorkerDatasetContext(TransactionContext context,
                                        LoadingCache<Long, Map<String, Dataset>> datasetsCache) {
      this.context = context;
      this.datasetsCache = datasetsCache;
    }

    /**
     * Get an instance of the specified Dataset. This method is thread-safe and may be used concurrently.
     * The returned dataset is also added to the transaction of the current {@link #execute(TxRunnable)} call.
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
     * The returned dataset is also added to the transaction of the current {@link #execute(TxRunnable)} call.
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

      if (!datasets.contains(name)) {
        throw new DatasetInstantiationException(
          String.format("Trying to access dataset '%s' that was not declared with " +
                          "useDataset() in the worker's configure()", name));
      }

      try {
        Map<String, Dataset> threadLocalMap = datasetsCache.get(Thread.currentThread().getId());
        Dataset dataset = threadLocalMap.get(name);
        if (dataset == null) {
          dataset = datasetFramework.getDataset(name, arguments, getProgram().getClassLoader());
          if (dataset != null) {
            threadLocalMap.put(name, dataset);
          }
        }

        if (dataset != null) {
          if (dataset instanceof TransactionAware) {
            context.addTransactionAware((TransactionAware) dataset);
          }

          @SuppressWarnings("unchecked")
          T resultDataset = (T) dataset;
          return resultDataset;
        }
      } catch (Throwable t) {
        throw new DatasetInstantiationException(String.format("Could not instantiate dataset '%s'", name), t);
      }
      // if it gets here, then the dataset was null
      throw new DatasetInstantiationException(String.format("Dataset '%s' does not exist", name));
    }
  }
}
