
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetCacheKey;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetContext;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * The MapReduce program runtime context for use in the {@link AbstractMapReduce#beforeSubmit(MapReduceContext)} and
 * {@link AbstractMapReduce#onFinish(boolean, MapReduceContext)} methods.
 *
 * <p>
 * The runtime context allows the retrieval of datasets that were not specified in the application specification,
 * letting users access datasets at runtime that aren't known about at compile time.
 * </p>
 */
public class DynamicMapReduceContext extends BasicMapReduceContext implements DatasetContext {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicMapReduceContext.class);

  private final LoadingCache<Long, Map<DatasetCacheKey, Dataset>> datasetsCache;
  private final TransactionContext txContext;
  private final DynamicDatasetContext dynamicDatasetContext;

  public DynamicMapReduceContext(Program program,
                                 MapReduceMetrics.TaskType type,
                                 RunId runId, String taskId,
                                 Arguments runtimeArguments,
                                 MapReduceSpecification spec,
                                 long logicalStartTime, String workflowBatch,
                                 DiscoveryServiceClient discoveryServiceClient,
                                 MetricsCollectionService metricsCollectionService,
                                 TransactionSystemClient txClient,
                                 DatasetFramework dsFramework) {
    super(program, type, runId, taskId, runtimeArguments, Collections.<String>emptySet(), spec,
          logicalStartTime, workflowBatch, discoveryServiceClient, metricsCollectionService, dsFramework);
    this.datasetsCache = CacheBuilder.newBuilder()
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
    this.txContext = new TransactionContext(txClient);
    this.dynamicDatasetContext = new DynamicDatasetContext(Id.Namespace.from(getNamespaceId()),
                                                           txContext, dsFramework,
                                                           program.getClassLoader(), null,
                                                           runtimeArguments.asMap()) {
      @Nullable
      @Override
      protected LoadingCache<Long, Map<DatasetCacheKey, Dataset>> getDatasetsCache() {
        return datasetsCache;
      }
    };
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return dynamicDatasetContext.getDataset(name, arguments);
  }

  public void close() {
    datasetsCache.invalidateAll();
    datasetsCache.cleanUp();
    super.close();
  }

  public TransactionContext getTransactionContext() {
    return txContext;
  }
}
