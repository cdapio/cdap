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

package co.cask.cdap.internal.app.runtime.worker;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.dataset2.DatasetCacheKey;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetContext;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.logging.context.WorkerLoggingContext;
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
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Default implementation of {@link WorkerContext}
 */
public class BasicWorkerContext extends AbstractContext implements WorkerContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicWorkerContext.class);

  private final WorkerSpecification specification;
  private final TransactionSystemClient transactionSystemClient;
  private final DatasetFramework datasetFramework;
  private final Metrics userMetrics;
  private final int instanceId;
  private final LoggingContext loggingContext;
  private volatile int instanceCount;
  private final LoadingCache<Long, Map<DatasetCacheKey, Dataset>> datasetsCache;
  private final Program program;
  private final Map<String, String> runtimeArgs;
  private final StreamWriter streamWriter;
  private final Map<String, Plugin> plugins;

  public BasicWorkerContext(WorkerSpecification spec, Program program, RunId runId, int instanceId,
                            int instanceCount, Arguments runtimeArgs, CConfiguration cConf,
                            MetricsCollectionService metricsCollectionService,
                            DatasetFramework datasetFramework,
                            TransactionSystemClient transactionSystemClient,
                            DiscoveryServiceClient discoveryServiceClient,
                            StreamWriterFactory streamWriterFactory,
                            @Nullable PluginInstantiator pluginInstantiator) {
    super(program, runId, runtimeArgs, spec.getDatasets(),
          getMetricCollector(program, runId.getId(), instanceId, metricsCollectionService),
          datasetFramework, discoveryServiceClient, pluginInstantiator);
    this.program = program;
    this.specification = spec;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.transactionSystemClient = transactionSystemClient;
    this.datasetFramework = datasetFramework;
    this.loggingContext = createLoggingContext(program.getId(), runId);
    if (metricsCollectionService != null) {
      this.userMetrics = new ProgramUserMetrics(getProgramMetrics());
    } else {
      this.userMetrics = null;
    }
    this.runtimeArgs = runtimeArgs.asMap();
    this.streamWriter = streamWriterFactory.create(new Id.Run(program.getId(), runId.getId()), getOwners());
    this.plugins = Maps.newHashMap(program.getApplicationSpecification().getPlugins());

    // The cache expiry should be greater than (2 * transaction.timeout) and at least 2 hours.
    // This ensures that when a dataset instance is requested multiple times during a single transaction,
    // the same instance is always returned.
    long cacheExpiryTimeout =
      Math.max(2, 2 * TimeUnit.SECONDS.toHours(cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT,
                                                            TxConstants.Manager.DEFAULT_TX_TIMEOUT)));
    // A cache of datasets by threadId. Repeated requests for a dataset from the same thread returns the same
    // instance, thus avoiding the overhead of creating a new instance for every request.
    this.datasetsCache = CacheBuilder.newBuilder()
      .expireAfterAccess(cacheExpiryTimeout, TimeUnit.HOURS)
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

  private LoggingContext createLoggingContext(Id.Program programId, RunId runId) {
    return new WorkerLoggingContext(programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                                    runId.getId(), String.valueOf(getInstanceId()));
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Nullable
  private static MetricsContext getMetricCollector(Program program, String runId, int instanceId,
                                                     @Nullable MetricsCollectionService service) {
    if (service == null) {
      return null;
    }
    Map<String, String> tags = Maps.newHashMap(getMetricsContext(program, runId));
    tags.put(Constants.Metrics.Tag.INSTANCE_ID, String.valueOf(instanceId));

    return service.getContext(tags);
  }

  @Override
  public WorkerSpecification getSpecification() {
    return specification;
  }

  @Override
  public void execute(TxRunnable runnable) {
    final TransactionContext context = new TransactionContext(transactionSystemClient);
    try {
      context.start();
      runnable.run(new DynamicDatasetContext(Id.Namespace.from(program.getNamespaceId()),
                                             context, getProgramMetrics(), datasetFramework,
                                             getProgram().getClassLoader(), runtimeArgs, null, getOwners()) {
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
    Closeables.closeQuietly(getPluginInstantiator());
  }

  @Override
  public Map<String, Plugin> getPlugins() {
    return plugins;
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

  @Override
  public void write(String stream, String data) throws IOException {
    streamWriter.write(stream, data);
  }

  @Override
  public void write(String stream, String data, Map<String, String> headers) throws IOException {
    streamWriter.write(stream, data, headers);
  }

  @Override
  public void write(String stream, ByteBuffer data) throws IOException {
    streamWriter.write(stream, data);
  }

  @Override
  public void write(String stream, StreamEventData data) throws IOException {
    streamWriter.write(stream, data);
  }

  @Override
  public void writeFile(String stream, File file, String contentType) throws IOException {
    streamWriter.writeFile(stream, file, contentType);
  }

  @Override
  public StreamBatchWriter createBatchWriter(String stream, String contentType) throws IOException {
    return streamWriter.createBatchWriter(stream, contentType);
  }
}
