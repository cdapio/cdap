/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.condition;

import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.workflow.AbstractCondition;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.condition.Condition;
import io.cdap.cdap.etl.api.condition.ConditionContext;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.SetMultimapCodec;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents conditions in the data pipelines.
 */
public class PipelineCondition extends AbstractCondition {

  // This is only visible during the configure time, not at runtime.
  private final BatchPhaseSpec phaseSpec;
  private Metrics metrics;

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .create();

  public PipelineCondition(BatchPhaseSpec spec) {
    this.phaseSpec = spec;
  }

  @Override
  protected void configure() {
    setName(phaseSpec.getPhaseName());
    setDescription("Condition phase executor. " + phaseSpec.getPhaseName());

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec));
    setProperties(properties);
  }

  @Override
  public boolean apply(@Nullable WorkflowContext context) {
    if (context == null) {
      // should not happen
      throw new IllegalStateException("WorkflowContext for the Condition cannot be null.");
    }
    Map<String, String> properties = context.getConditionSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
    PipelinePhase phase = phaseSpec.getPhase();
    StageSpec stageSpec = phase.iterator().next();
    PluginContext pluginContext = new PipelinePluginContext(context, metrics, phaseSpec.isStageLoggingEnabled(),
                                                            phaseSpec.isProcessTimingEnabled());

    MacroEvaluator macroEvaluator =
      new DefaultMacroEvaluator(new BasicArguments(context.getToken(), context.getRuntimeArguments()),
                                context.getLogicalStartTime(), context, context, context.getNamespace());

    try {
      Condition condition = pluginContext.newPluginInstance(stageSpec.getName(), macroEvaluator);
      PipelineRuntime pipelineRuntime = new PipelineRuntime(context, metrics);
      ConditionContext conditionContext = new BasicConditionContext(context, pipelineRuntime, stageSpec);
      boolean result = condition.apply(conditionContext);
      WorkflowToken token = context.getToken();
      if (token == null) {
        throw new IllegalStateException("WorkflowToken cannot be null when Condition is executed through Workflow.");
      }

      for (Map.Entry<String, String> entry : pipelineRuntime.getArguments().getAddedArguments().entrySet()) {
        token.put(entry.getKey(), entry.getValue());
      }

      return result;
    } catch (Exception e) {
      String msg = String.format("Error executing condition '%s' in the pipeline.", stageSpec.getName());
      throw new RuntimeException(msg, e);
    }
  }
}
