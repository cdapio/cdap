/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Internal implementation of {@link FlowletContext}.
 */
final class BasicFlowletContext extends AbstractContext implements FlowletContext {

  private final String flowId;
  private final String flowletId;
  private final long groupId;
  private final int instanceId;
  private final FlowletSpecification flowletSpec;

  private volatile int instanceCount;
  private final LoadingCache<String, MetricsContext> queueMetrics;
  private final LoadingCache<ImmutablePair<String, String>, MetricsContext> producerMetrics;

  BasicFlowletContext(Program program, ProgramOptions programOptions, final String flowletId,
                      int instanceId, int instanceCount, Set<String> datasets,
                      FlowletSpecification flowletSpec,
                      MetricsCollectionService metricsService,
                      DiscoveryServiceClient discoveryServiceClient,
                      TransactionSystemClient txClient,
                      DatasetFramework dsFramework,
                      SecureStore secureStore,
                      SecureStoreManager secureStoreManager) {
    super(program, programOptions, datasets, dsFramework, txClient, discoveryServiceClient, false, metricsService,
          ImmutableMap.of(Constants.Metrics.Tag.FLOWLET, flowletId,
                          Constants.Metrics.Tag.INSTANCE_ID, String.valueOf(instanceId)),
          secureStore, secureStoreManager
    );

    this.flowId = program.getName();
    this.flowletId = flowletId;
    this.groupId = FlowUtils.generateConsumerGroupId(program.getId().toEntityId(), flowletId);
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.flowletSpec = flowletSpec;

    //noinspection NullableProblems
    this.queueMetrics = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<String, MetricsContext>() {
        @Override
        public MetricsContext load(String key) throws Exception {
          return getProgramMetrics().childContext(Constants.Metrics.Tag.FLOWLET_QUEUE, key);
        }
      });

    //noinspection NullableProblems
    this.producerMetrics = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<ImmutablePair<String, String>, MetricsContext>() {
        @Override
        public MetricsContext load(ImmutablePair<String, String> key) throws Exception {
          return getProgramMetrics()
            .childContext(ImmutableMap.of(Constants.Metrics.Tag.PRODUCER, key.getFirst(),
                                          Constants.Metrics.Tag.FLOWLET_QUEUE, key.getSecond(),
                                          Constants.Metrics.Tag.CONSUMER, BasicFlowletContext.this.flowletId));
        }
      });

  }

  @Override
  public String toString() {
    return String.format("flowlet=%s, instance=%d, groupsize=%s, %s",
                         getFlowletId(), getInstanceId(), getInstanceCount(), super.toString());
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  @Override
  public String getName() {
    return getFlowletId();
  }

  @Override
  public FlowletSpecification getSpecification() {
    return flowletSpec;
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }

  public String getFlowId() {
    return flowId;
  }

  public String getFlowletId() {
    return flowletId;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  public LoggingContext getLoggingContext() {
    return new FlowletLoggingContext(getNamespaceId(), getApplicationId(), getFlowId(), getFlowletId(),
                                     getRunId().getId(), String.valueOf(getInstanceId()));
  }

  public MetricsContext getQueueMetrics(String flowletQueueName) {
    return queueMetrics.getUnchecked(flowletQueueName);
  }

  public MetricsContext getProducerMetrics(ImmutablePair<String, String> producerAndQueue) {
    return producerMetrics.getUnchecked(producerAndQueue);
  }

  public long getGroupId() {
    return groupId;
  }
}
