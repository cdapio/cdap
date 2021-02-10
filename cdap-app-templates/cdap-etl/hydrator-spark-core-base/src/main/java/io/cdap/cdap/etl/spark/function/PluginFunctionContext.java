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

package io.cdap.cdap.etl.spark.function;

import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.StageLifecycle;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.batch.connector.SingleConnectorSink;
import io.cdap.cdap.etl.batch.connector.SingleConnectorSource;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAutoJoinerContext;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.batch.SparkBatchRuntimeContext;
import io.cdap.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import io.cdap.cdap.etl.validation.LoggingFailureCollector;

import java.io.Serializable;
import java.util.Map;

/**
 * Serializable collection of objects that can be used in Spark closures to instantiate plugins.
 */
public class PluginFunctionContext implements Serializable {
  private static final long serialVersionUID = -7897960584858589315L;
  
  private final String namespace;
  private final String pipelineName;
  private final long logicalStartTime;
  private final BasicArguments arguments;
  private final PluginContext pluginContext;
  private final ServiceDiscoverer serviceDiscoverer;
  private final Metrics metrics;
  private final SecureStore secureStore;
  private final DataTracer dataTracer;
  private final StageSpec stageSpec;
  private final StageStatisticsCollector collector;
  private transient PipelinePluginContext pipelinePluginContext;

  public PluginFunctionContext(StageSpec stageSpec, JavaSparkExecutionContext sec, StageStatisticsCollector collector) {
    this(stageSpec, sec, new BasicArguments(sec).asMap(), sec.getLogicalStartTime(), collector);
  }

  // used in spark streaming, where each batch has a different batch time, and prepareRun is run per batch
  public PluginFunctionContext(StageSpec stageSpec, JavaSparkExecutionContext sec, Map<String, String> arguments,
                               long logicalStartTime, StageStatisticsCollector collector) {
    this.namespace = sec.getNamespace();
    this.pipelineName = sec.getApplicationSpecification().getName();
    this.stageSpec = stageSpec;
    this.logicalStartTime = logicalStartTime;
    this.arguments = new BasicArguments(sec);
    this.pluginContext = sec.getPluginContext();
    this.serviceDiscoverer = sec.getServiceDiscoverer();
    this.metrics = sec.getMetrics();
    this.secureStore = sec.getSecureStore();
    this.dataTracer = sec.getDataTracer(stageSpec.getName());
    this.pipelinePluginContext = getPluginContext();
    this.collector = collector;
  }

  public <T> T createPlugin() throws Exception {
    if (Constants.Connector.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
      String connectorType = stageSpec.getPlugin().getProperties().get(Constants.Connector.TYPE);
      // ok to pass in null to constructors here since we are only going to use the transform method
      if (connectorType.equals(Constants.Connector.SOURCE_TYPE)) {
        return (T) new SingleConnectorSource(null, null);
      } else {
        return (T) new SingleConnectorSink(null, null);
      }
    }
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(arguments, logicalStartTime, secureStore, namespace);
    return getPluginContext().newPluginInstance(stageSpec.getName(), macroEvaluator);
  }

  public <CTX, T extends StageLifecycle<? super CTX>> T createAndInitializePlugin(CTX context, FunctionCache cache)
    throws Exception {
    return cache.getValue(() -> {
      T plugin = createPlugin();
      plugin.initialize(context);
      return plugin;
    });
  }

  public <T extends StageLifecycle<? super SparkBatchRuntimeContext>> T createAndInitializePlugin(FunctionCache cache)
    throws Exception {
    return createAndInitializePlugin(createBatchRuntimeContext(), cache);
  }

  public AutoJoinerContext createAutoJoinerContext() {
    FailureCollector failureCollector = new LoggingFailureCollector(getStageName(), stageSpec.getInputSchemas());
    return DefaultAutoJoinerContext.from(stageSpec.getInputSchemas(), failureCollector);
  }

  public String getStageName() {
    return stageSpec.getName();
  }

  public StageSpec getStageSpec() {
    return stageSpec;
  }

  public StageMetrics createStageMetrics() {
    return new DefaultStageMetrics(metrics, stageSpec.getName());
  }

  public StageStatisticsCollector getStageStatisticsCollector() {
    return collector;
  }

  public SparkBatchRuntimeContext createBatchRuntimeContext() {
    PipelineRuntime pipelineRuntime = new PipelineRuntime(namespace, pipelineName, logicalStartTime,
                                                          arguments, metrics, pluginContext,
                                                          serviceDiscoverer, secureStore);
    return new SparkBatchRuntimeContext(pipelineRuntime, stageSpec);
  }

  public DataTracer getDataTracer() {
    return dataTracer;
  }

  private PipelinePluginContext getPluginContext() {
    if (pipelinePluginContext == null) {
      pipelinePluginContext = new SparkPipelinePluginContext(pluginContext, metrics,
                                                             stageSpec.isStageLoggingEnabled(),
                                                             stageSpec.isProcessTimingEnabled());
    }
    return pipelinePluginContext;
  }
}
