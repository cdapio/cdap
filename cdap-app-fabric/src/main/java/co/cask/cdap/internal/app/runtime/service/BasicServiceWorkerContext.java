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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.ServiceWorkerContext;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetCacheKey;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetContext;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
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
  private final Metrics userMetrics;
  private final int instanceId;
  private volatile int instanceCount;
  private final LoadingCache<Long, Map<DatasetCacheKey, Dataset>> datasetsCache;
  private final Program program;
  private final Map<String, String> runtimeArgs;

  public BasicServiceWorkerContext(ServiceWorkerSpecification spec, Program program, RunId runId, int instanceId,
                                   int instanceCount, Arguments runtimeArgs, CConfiguration cConf,
                                   MetricsCollectionService metricsCollectionService,
                                   DatasetFramework datasetFramework,
                                   TransactionSystemClient transactionSystemClient,
                                   DiscoveryServiceClient discoveryServiceClient) {
    super(program, runId, runtimeArgs, spec.getDatasets(),
          getMetricCollector(metricsCollectionService, program, spec.getName(), runId.getId(), instanceId),
          datasetFramework, cConf, discoveryServiceClient);
    this.program = program;
    this.specification = spec;
    this.datasets = ImmutableSet.copyOf(spec.getDatasets());
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.transactionSystemClient = transactionSystemClient;
    this.datasetFramework = new NamespacedDatasetFramework(datasetFramework,
                                                           new DefaultDatasetNamespace(cConf, Namespace.USER));
    this.userMetrics = new ProgramUserMetrics(getMetricCollector(metricsCollectionService, program,
                                                                 spec.getName(), runId.getId(), instanceId));
    this.runtimeArgs = runtimeArgs.asMap();

    // The cache expiry should be greater than (2 * transaction.timeout) and at least 2 minutes.
    // This ensures that when a dataset instance is requested multiple times during a single transaction,
    // the same instance is always returned.
    long cacheExpiryTimeout =
      Math.max(2, 2 * TimeUnit.SECONDS.toMinutes(cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT,
                                                              TxConstants.Manager.DEFAULT_TX_TIMEOUT)));
    // A cache of datasets by threadId. Repeated requests for a dataset from the same thread returns the same
    // instance, thus avoiding the overhead of creating a new instance for every request.
    this.datasetsCache = CacheBuilder.newBuilder()
      .expireAfterAccess(cacheExpiryTimeout, TimeUnit.MINUTES)
      .removalListener(new RemovalListener<Long, Map<DatasetCacheKey, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<Long, Map<DatasetCacheKey, Dataset>> notification) {
          if (notification.getValue() != null) {
            for (Map.Entry<DatasetCacheKey, Dataset> entry : notification.getValue().entrySet()) {
              try {
                entry.getValue().close();
              } catch (IOException e) {
                LOG.error("Error closing dataset: {}", entry.getKey(), e);
              }
            }
          }
        }
      })
      .build(new CacheLoader<Long, Map<DatasetCacheKey, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public Map<DatasetCacheKey, Dataset> load(Long key) throws Exception {
          return Maps.newHashMap();
        }
      });
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  public LoggingContext getLoggingContext() {
    return new UserServiceLoggingContext(program.getNamespaceId(), program.getApplicationId(),
                                         program.getId().getId(), specification.getName());
  }

  private static MetricsCollector getMetricCollector(MetricsCollectionService service, Program program,
                                                     String runnableName, String runId, int instanceId) {
    if (service == null) {
      return null;
    }
    Map<String, String> tags = Maps.newHashMap(getMetricsContext(program, runId));
    tags.put(MetricTags.SERVICE_RUNNABLE.getCodeName(), runnableName);
    tags.put(MetricTags.INSTANCE_ID.getCodeName(), String.valueOf(instanceId));
    return service.getCollector(tags);
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
      runnable.run(new DynamicDatasetContext(Id.Namespace.from(program.getNamespaceId()), context, datasetFramework,
                                             getProgram().getClassLoader(), datasets, runtimeArgs) {
        @Override
        protected LoadingCache<Long, Map<DatasetCacheKey, Dataset>> getDatasetsCache() {
          return datasetsCache;
        }
      });
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

  public void setInstanceCount(int instanceCount) {
    this.instanceCount = instanceCount;
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
}
