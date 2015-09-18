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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.batch.dataset.MultipleOutputs;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Mapreduce task runtime context which delegates to BasicMapReduceContext for non task-specific methods.
 * It currently also extends MapReduceContext to support backwards compatibility. Mapper and Reducer tasks could
 * implement ProgramLifeCycle<MapReduceContext> in order to to get a MapReduceContext object.
 *
 * @param <KEYOUT>   output key type
 * @param <VALUEOUT> output value type
 */
public class BasicMapReduceTaskContext<KEYOUT, VALUEOUT> extends AbstractContext
  implements MapReduceTaskContext<KEYOUT, VALUEOUT>, Closeable {

  private final MapReduceSpecification spec;
  private final LoggingContext loggingContext;
  private final long logicalStartTime;
  private final WorkflowToken workflowToken;
  private final Metrics userMetrics;
  private final Map<String, Plugin> plugins;

  private MultipleOutputs multipleOutputs;
  private TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;

  public BasicMapReduceTaskContext(Program program,
                                   @Nullable MapReduceMetrics.TaskType type,
                                   RunId runId, String taskId,
                                   Arguments runtimeArguments,
                                   Set<String> datasets,
                                   MapReduceSpecification spec,
                                   long logicalStartTime,
                                   @Nullable WorkflowToken workflowToken,
                                   DiscoveryServiceClient discoveryServiceClient,
                                   MetricsCollectionService metricsCollectionService,
                                   DatasetFramework dsFramework,
                                   PluginInstantiator pluginInstantiator) {
    super(program, runId, runtimeArguments, datasets,
          getMetricCollector(program, runId.getId(), taskId, metricsCollectionService, type),
          dsFramework, discoveryServiceClient, pluginInstantiator);
    this.logicalStartTime = logicalStartTime;
    this.workflowToken = workflowToken;

    if (metricsCollectionService != null) {
      this.userMetrics = new ProgramUserMetrics(getProgramMetrics());
    } else {
      this.userMetrics = null;
    }
    this.loggingContext = createLoggingContext(program.getId(), runId);
    this.spec = spec;
    this.plugins = Maps.newHashMap(program.getApplicationSpecification().getPlugins());
  }

  private LoggingContext createLoggingContext(Id.Program programId, RunId runId) {
    return new MapReduceLoggingContext(programId.getNamespaceId(), programId.getApplicationId(),
                                       programId.getId(), runId.getId());
  }

  @Override
  public String toString() {
    return String.format("job=%s,=%s", spec.getName(), super.toString());
  }

  @Override
  public Map<String, Plugin> getPlugins() {
    return plugins;
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
    if (multipleOutputs == null) {
      throw new IOException("MultipleOutputs has not been initialized.");
    }
    multipleOutputs.write(namedOutput, key, value);
  }

  @Override
  public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
    if (multipleOutputs == null) {
      throw new IOException("Hadoop context has not been initialized.");
    }
    context.write(key, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getHadoopContext() {
    return (T) context;
  }

  public void setHadoopContext(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
    this.multipleOutputs = new MultipleOutputs(context);
    this.context = context;
  }

  @Override
  public void close() {
    if (multipleOutputs != null) {
      multipleOutputs.close();
    }
    super.close();
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
   * Returns the WorkflowToken if the MapReduce program is executed as a part of the Workflow.
   */
  @Override
  @Nullable
  public WorkflowToken getWorkflowToken() {
    return workflowToken;
  }

  @Nullable
  private static MetricsContext getMetricCollector(Program program, String runId, String taskId,
                                                   @Nullable MetricsCollectionService service,
                                                   @Nullable MapReduceMetrics.TaskType type) {
    if (service == null) {
      return null;
    }

    Map<String, String> tags = Maps.newHashMap();
    tags.putAll(getMetricsContext(program, runId));
    if (type != null) {
      tags.put(Constants.Metrics.Tag.MR_TASK_TYPE, type.getId());
      tags.put(Constants.Metrics.Tag.INSTANCE_ID, taskId);
    }

    return service.getContext(tags);
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  public void flushOperations() throws Exception {
    for (TransactionAware txAware : getDatasetInstantiator().getTransactionAware()) {
      txAware.commitTx();
    }
  }
}
