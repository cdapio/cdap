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

package co.cask.cdap.etl.spark.function;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.batch.SparkBatchRuntimeContext;
import co.cask.cdap.etl.spark.batch.SparkJoinerRuntimeContext;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializable collection of objects that can be used in Spark closures to instantiate plugins.
 */
public class PluginFunctionContext implements Serializable {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .create();
  private static final long serialVersionUID = -8131461628444037900L;

  private final String stageName;
  private final PluginContext pluginContext;
  private final Metrics metrics;
  private final long logicalStartTime;
  private final Map<String, String> arguments;
  private final String pipelineStr;
  private final SecureStore secureStore;
  private final String namespace;

  public PluginFunctionContext(String stageName, JavaSparkExecutionContext sec, PipelinePhase pipelinePhase) {
    this.stageName = stageName;
    this.pluginContext = sec.getPluginContext();
    this.metrics = sec.getMetrics();
    this.logicalStartTime = sec.getLogicalStartTime();
    Map<String, String> arguments = new HashMap<>();
    arguments.putAll(sec.getRuntimeArguments());
    WorkflowToken token = sec.getWorkflowToken();
    if (token != null) {
      for (String tokenKey : token.getAll(WorkflowToken.Scope.USER).keySet()) {
        arguments.put(tokenKey, token.get(tokenKey, WorkflowToken.Scope.USER).toString());
      }
    }
    this.arguments = arguments;
    this.pipelineStr = GSON.toJson(pipelinePhase);
    this.secureStore = sec.getSecureStore();
    this.namespace = sec.getNamespace();
  }

  public <T> T createPlugin() throws Exception {
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(arguments, logicalStartTime, secureStore, namespace);
    return pluginContext.newPluginInstance(stageName, macroEvaluator);
  }

  public String getStageName() {
    return stageName;
  }

  public StageMetrics createStageMetrics() {
    return new DefaultStageMetrics(metrics, stageName);
  }

  public BatchRuntimeContext createBatchRuntimeContext() {
    return new SparkBatchRuntimeContext(pluginContext, metrics, logicalStartTime, arguments, stageName);
  }

  public BatchJoinerRuntimeContext createJoinerRuntimeContext() {
    PipelinePhase pipelinePhase = GSON.fromJson(pipelineStr, PipelinePhase.class);
    StageInfo stageInfo = pipelinePhase.getStage(stageName);
    return new SparkJoinerRuntimeContext(pluginContext, metrics, logicalStartTime, arguments,
      stageName, stageInfo.getInputSchemas(), stageInfo.getOutputSchema());
  }
}
