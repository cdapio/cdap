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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.ProgramServiceDiscovery;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import com.continuuity.tephra.TransactionAware;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Job;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.ServiceDiscovered;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mapreduce job runtime context
 */
public class BasicMapReduceContext extends AbstractContext implements MapReduceContext {

  private final String accountId;
  private final MapReduceSpecification spec;
  private final MapReduceLoggingContext loggingContext;
  private final Map<MetricsScope, MetricsCollector> systemMapperMetrics;
  private final Map<MetricsScope, MetricsCollector> systemReducerMetrics;
  private final Map<MetricsScope, MetricsCollector> systemMetrics;
  private final Arguments runtimeArguments;
  private final long logicalStartTime;
  private final String workflowBatch;
  private final Metrics mapredMetrics;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramServiceDiscovery serviceDiscovery;

  private String inputDatasetName;
  private List<Split> inputDataSelection;

  private String outputDatasetName;
  private Job job;

  // todo: having it here seems like a hack will be fixed with further post-integration refactoring
  private final Iterable<TransactionAware> txAwares;

  public BasicMapReduceContext(Program program,
                               MapReduceMetrics.TaskType type,
                               RunId runId,
                               Arguments runtimeArguments,
                               Map<String, Closeable> datasets,
                               MapReduceSpecification spec,
                               Iterable<TransactionAware> txAwares,
                               long logicalStartTime,
                               String workflowBatch,
                               ProgramServiceDiscovery serviceDiscovery) {
    this(program, type, runId, runtimeArguments, datasets,
         spec, txAwares, logicalStartTime, workflowBatch, serviceDiscovery, null);
  }


  public BasicMapReduceContext(Program program,
                               MapReduceMetrics.TaskType type,
                               RunId runId,
                               Arguments runtimeArguments,
                               Map<String, Closeable> datasets,
                               MapReduceSpecification spec,
                               Iterable<TransactionAware> txAwares,
                               long logicalStartTime,
                               String workflowBatch,
                               ProgramServiceDiscovery serviceDiscovery,
                               MetricsCollectionService metricsCollectionService) {
    super(program, runId, datasets);
    this.accountId = program.getAccountId();
    this.runtimeArguments = runtimeArguments;
    this.logicalStartTime = logicalStartTime;
    this.workflowBatch = workflowBatch;
    this.serviceDiscovery = serviceDiscovery;
    this.metricsCollectionService = metricsCollectionService;

    if (metricsCollectionService != null) {
      this.systemMapperMetrics = Maps.newHashMap();
      this.systemReducerMetrics = Maps.newHashMap();
      this.systemMetrics = Maps.newHashMap();
      for (MetricsScope scope : MetricsScope.values()) {
        this.systemMapperMetrics.put(scope, getMetricsCollector(scope, metricsCollectionService,
                                                                getMetricContext(MapReduceMetrics.TaskType.Mapper)));
        this.systemReducerMetrics.put(scope, getMetricsCollector(scope, metricsCollectionService,
                                                                 getMetricContext(MapReduceMetrics.TaskType.Reducer)));
        this.systemMetrics.put(scope, getMetricsCollector(scope, metricsCollectionService,
                                                                 getMetricContext()));
      }
      // for user metrics.  type can be null if its not in a map or reduce task, but in the yarn container that
      // launches the mapred job.
      this.mapredMetrics = (type == null) ?
        null : new MapReduceMetrics(metricsCollectionService, getApplicationId(), getProgramName(), type);
    } else {
      this.systemMapperMetrics = null;
      this.systemReducerMetrics = null;
      this.systemMetrics = null;
      this.mapredMetrics = null;
    }
    this.loggingContext = new MapReduceLoggingContext(getAccountId(), getApplicationId(), getProgramName());
    this.spec = spec;
    this.txAwares = txAwares;
  }

  @Override
  public String toString() {
    return String.format("job=%s,=%s",
                         spec.getName(), super.toString());
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return spec;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  /**
   * Returns the name of the Batch job when running inside workflow. Otherwise, return null.
   */
  public String getWorkflowBatch() {
    return workflowBatch;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getHadoopJob() {
    return (T) job;
  }

  @Override
  public void setInput(String datasetName, List<Split> splits) {
    this.inputDatasetName = datasetName;
    this.inputDataSelection = splits;
  }

  @Override
  public void setOutput(String datasetName) {
    this.outputDatasetName = datasetName;
  }

  public int getInstanceId() {
    return 0;
  }

  private String getMetricContext(MapReduceMetrics.TaskType type) {
    return String.format("%s.b.%s.%s.%d",
                         getApplicationId(),
                         getProgramName(),
                         type.getId(),
                         getInstanceId());
  }

  private String getMetricContext() {
    return String.format("%s.b.%s.%d",
                         getApplicationId(),
                         getProgramName(),
                         getInstanceId());
  }

  @Override
  public Metrics getMetrics() {
    return mapredMetrics;
  }

  public MetricsCollectionService getMetricsCollectionService() {
    return metricsCollectionService;
  }

  public MetricsCollector getSystemMetrics(MetricsScope scope) {
    return systemMetrics.get(scope);
  }

  public MetricsCollector getSystemMapperMetrics() {
    return systemMapperMetrics.get(MetricsScope.REACTOR);
  }

  public MetricsCollector getSystemReducerMetrics() {
    return systemReducerMetrics.get(MetricsScope.REACTOR);
  }

  public MetricsCollector getSystemMapperMetrics(MetricsScope scope) {
    return systemMapperMetrics.get(scope);
  }

  public MetricsCollector getSystemReducerMetrics(MetricsScope scope) {
    return systemReducerMetrics.get(scope);
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Nullable
  public String getInputDatasetName() {
    return inputDatasetName;
  }

  public List<Split> getInputDataSelection() {
    return inputDataSelection;
  }

  @Nullable
  public String getOutputDatasetName() {
    return outputDatasetName;
  }

  Arguments getRuntimeArgs() {
    return runtimeArguments;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    ImmutableMap.Builder<String, String> arguments = ImmutableMap.builder();
    Iterator<Map.Entry<String, String>> it = runtimeArguments.iterator();
    while (it.hasNext()) {
      arguments.put(it.next());
    }
    return arguments.build();
  }

  @Override
  public ServiceDiscovered discover(String appId, String serviceId, String serviceName) {
    return serviceDiscovery.discover(accountId, appId, serviceId, serviceName);
  }

  public void flushOperations() throws Exception {
    for (TransactionAware txAware : txAwares) {
      txAware.commitTx();
    }
  }
}
