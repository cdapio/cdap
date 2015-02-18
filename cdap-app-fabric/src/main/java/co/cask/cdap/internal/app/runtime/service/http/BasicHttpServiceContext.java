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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation of HttpServiceContext which simply stores and retrieves the
 * spec provided when this class is instantiated
 */
public class BasicHttpServiceContext extends AbstractContext implements TransactionalHttpServiceContext {

  private final HttpServiceHandlerSpecification spec;
  private final TransactionContext txContext;
  private final Metrics userMetrics;
  private final int instanceId;
  private final AtomicInteger instanceCount;

  /**
   * Creates a BasicHttpServiceContext for the given HttpServiceHandlerSpecification.
   * @param spec spec to create a context for.
   * @param program program of the context.
   * @param runId runId of the component.
   * @param instanceId instanceId of the component.
   * @param instanceCount total number of instances of the component.
   * @param runtimeArgs runtimeArgs for the component.
   * @param metricsCollectionService metricsCollectionService to use for emitting metrics.
   * @param dsFramework dsFramework to use for getting datasets.
   * @param conf CConfiguration of the system.
   * @param discoveryServiceClient discoveryServiceClient used to do service discovery.
   * @param txClient txClient to do transaction operations.
   */
  public BasicHttpServiceContext(HttpServiceHandlerSpecification spec,
                                 Program program, RunId runId, int instanceId, AtomicInteger instanceCount,
                                 Arguments runtimeArgs, MetricsCollectionService metricsCollectionService,
                                 DatasetFramework dsFramework, CConfiguration conf,
                                 DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient) {
    super(program, runId, runtimeArgs, spec.getDatasets(),
          getMetricCollector(metricsCollectionService, program, spec.getName(), runId.getId(), instanceId),
          dsFramework, conf, discoveryServiceClient);
    this.spec = spec;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.txContext = new TransactionContext(txClient, getDatasetInstantiator().getTransactionAware());
    this.userMetrics =
      new ProgramUserMetrics(getMetricCollector(metricsCollectionService, program,
                                                spec.getName(), runId.getId(), instanceId));
  }

  /**
   * @return the {@link HttpServiceHandlerSpecification} for this context
   */
  @Override
  public HttpServiceHandlerSpecification getSpecification() {
    return spec;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount.get();
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  @Override
  public TransactionContext getTransactionContext() {
    return txContext;
  }

  private static MetricsCollector getMetricCollector(MetricsCollectionService service,
                                                     Program program, String runnableName,
                                                     String runId, int instanceId) {
    if (service == null) {
      return null;
    }
    Map<String, String> tags = Maps.newHashMap(getMetricsContext(program, runId));
    tags.put(MetricTags.SERVICE_RUNNABLE.getCodeName(), runnableName);
    tags.put(MetricTags.INSTANCE_ID.getCodeName(), String.valueOf(instanceId));
    return service.getCollector(tags);
  }
}
