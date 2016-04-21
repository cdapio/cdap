/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.services.AbstractServiceDiscoverer;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.DefaultAdmin;
import co.cask.cdap.internal.app.runtime.DefaultPluginContext;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.logging.context.SparkLoggingContext;
import co.cask.cdap.logging.context.WorkflowProgramLoggingContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context to be used at Spark runtime to provide common functionality that are needed at both the driver and
 * the executors.
 */
public final class SparkRuntimeContext extends AbstractServiceDiscoverer
                                       implements RuntimeContext, Metrics, PluginContext, Closeable {

  private final Configuration hConf;
  private final Program program;
  private final RunId runId;
  private final Map<String, String> runtimeArguments;
  private final long logicalStartTime;
  private final TransactionSystemClient txClient;
  private final MultiThreadDatasetCache datasetCache;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MetricsContext metricsContext;
  private final Metrics userMetrics;
  private final StreamAdmin streamAdmin;
  private final WorkflowProgramInfo workflowProgramInfo;
  private final PluginInstantiator pluginInstantiator;
  private final PluginContext pluginContext;
  private final Admin admin;
  private final LoggingContext loggingContext;

  SparkRuntimeContext(Configuration hConf, Program program, RunId runId, Map<String, String> runtimeArguments,
                      TransactionSystemClient txClient,
                      DatasetFramework datasetFramework,
                      DiscoveryServiceClient discoveryServiceClient,
                      MetricsCollectionService metricsCollectionService,
                      StreamAdmin streamAdmin,
                      @Nullable WorkflowProgramInfo workflowProgramInfo,
                      @Nullable PluginInstantiator pluginInstantiator) {
    super(program.getId().toEntityId());

    this.hConf = hConf;
    this.program = program;
    this.runId = runId;

    Map<String, String> args = new HashMap<>(runtimeArguments);
    this.logicalStartTime = ProgramRunners.updateLogicalStartTime(args);
    this.runtimeArguments = Collections.unmodifiableMap(args);
    this.txClient = txClient;

    ProgramId programId = program.getId().toEntityId();
    this.metricsContext = createMetricsContext(metricsCollectionService, programId, runId, workflowProgramInfo);

    this.datasetCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework, program.getClassLoader(),
                                    Collections.singleton(programId.toId())),
      txClient, programId.getNamespaceId(), runtimeArguments, metricsContext, null);

    this.discoveryServiceClient = discoveryServiceClient;
    this.userMetrics = new ProgramUserMetrics(metricsContext);
    this.streamAdmin = streamAdmin;
    this.workflowProgramInfo = workflowProgramInfo;
    this.pluginInstantiator = pluginInstantiator;
    this.pluginContext = new DefaultPluginContext(pluginInstantiator, programId,
                                                  program.getApplicationSpecification().getPlugins());
    this.admin = new DefaultAdmin(datasetFramework, programId.getNamespaceId());
    this.loggingContext = createLoggingContext(programId, runId, workflowProgramInfo);
  }

  private LoggingContext createLoggingContext(ProgramId programId, RunId runId,
                                              @Nullable WorkflowProgramInfo workflowProgramInfo) {
    if (workflowProgramInfo == null) {
      return new SparkLoggingContext(programId.getNamespace(), programId.getApplication(), programId.getProgram(),
                                     runId.getId());
    }

    ProgramId workflowProramId = Ids.namespace(programId.getNamespace()).app(programId.getApplication())
      .workflow(workflowProgramInfo.getName());

    return new WorkflowProgramLoggingContext(workflowProramId.getNamespace(), workflowProramId.getApplication(),
                                             workflowProramId.getProgram(), workflowProgramInfo.getRunId().getId(),
                                             ProgramType.SPARK, programId.getProgram());
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return program.getApplicationSpecification();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  @Override
  public String getNamespace() {
    return program.getNamespaceId();
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public Admin getAdmin() {
    return admin;
  }

  @Override
  public void count(String metricName, int delta) {
    userMetrics.count(metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    userMetrics.gauge(metricName, value);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return pluginContext.getPluginProperties(pluginId);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return pluginContext.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return pluginContext.newPluginInstance(pluginId);
  }

  @Override
  protected DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  /**
   * Returns the {@link SparkSpecification} of the spark program of this context.
   */
  public SparkSpecification getSparkSpecification() {
    SparkSpecification spec = getApplicationSpecification().getSpark().get(getProgram().getName());
    // Spec shouldn't be null, otherwise the spark program won't even get started
    Preconditions.checkState(spec != null, "SparkSpecification not found for %s", getProgram().getId());
    return spec;
  }

  /**
   * Returns the {@link Program} of this context.
   */
  public Program getProgram() {
    return program;
  }

  /**
   * Returns the {@link WorkflowProgramInfo} if the spark program is running inside a workflow.
   */
  @Nullable
  public WorkflowProgramInfo getWorkflowInfo() {
    return workflowProgramInfo;
  }

  /**
   * Returns the logical start of this run.
   */
  long getLogicalStartTime() {
    return logicalStartTime;
  }

  /**
   * Returns the {@link TransactionSystemClient} for this execution.
   */
  TransactionSystemClient getTransactionSystemClient() {
    return txClient;
  }

  /**
   * Returns the {@link Configuration} used for the execution.
   */
  Configuration getConfiguration() {
    return hConf;
  }

  /**
   * Returns the {@link DynamicDatasetCache} to be used throughout the execution.
   */
  DynamicDatasetCache getDatasetCache() {
    return datasetCache;
  }

  /**
   * Returns the {@link PluginInstantiator} if plugin is used.
   */
  @Nullable
  PluginInstantiator getPluginInstantiator() {
    return pluginInstantiator;
  }

  /**
   * Returns the {@link LoggingContext} representing the program.
   */
  LoggingContext getLoggingContext() {
    return loggingContext;
  }

  /**
   * Returns the {@link MetricsContext} for the program. It can be used to emit either user or system metrics.
   */
  MetricsContext getMetricsContext() {
    return metricsContext;
  }

  /**
   * Returns the {@link StreamAdmin} used for this execution.
   */
  StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

  /**
   * Creates a {@link MetricsContext} to be used for the Spark execution.
   */
  private static MetricsContext createMetricsContext(MetricsCollectionService service,
                                                     ProgramId programId, RunId runId,
                                                     @Nullable WorkflowProgramInfo workflowProgramInfo) {
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, programId.getNamespace());
    tags.put(Constants.Metrics.Tag.APP, programId.getApplication());
    tags.put(ProgramTypeMetricTag.getTagName(ProgramType.SPARK), programId.getProgram());
    tags.put(Constants.Metrics.Tag.RUN_ID, runId.getId());

    // todo: use proper spark instance id. For now we have to emit smth for test framework's waitFor metric to work
    tags.put(Constants.Metrics.Tag.INSTANCE_ID, "0");

    if (workflowProgramInfo != null) {
      // If running inside Workflow, add the WorkflowMetricsContext as well
      tags.put(Constants.Metrics.Tag.WORKFLOW, workflowProgramInfo.getName());
      tags.put(Constants.Metrics.Tag.WORKFLOW_RUN_ID, workflowProgramInfo.getRunId().getId());
      tags.put(Constants.Metrics.Tag.NODE, workflowProgramInfo.getNodeId());

    }
    return service.getContext(tags);
  }

  @Override
  public void close() throws IOException {
    datasetCache.close();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(SparkRuntimeContext.class)
      .add("id", getProgram().getId())
      .add("runId", getRunId())
      .toString();
  }
}
