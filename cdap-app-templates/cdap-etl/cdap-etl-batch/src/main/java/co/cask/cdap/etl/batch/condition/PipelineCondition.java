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

package co.cask.cdap.etl.batch.condition;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.workflow.AbstractCondition;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.api.condition.ConditionContext;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
  public boolean apply(@Nullable WorkflowContext input) {
    if (input == null) {
      // should not happen
      throw new IllegalStateException("WorkflowContext for the Condition cannot be null.");
    }
    Map<String, String> properties = input.getConditionSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
    PipelinePhase phase = phaseSpec.getPhase();
    StageSpec stageSpec = phase.iterator().next();
    PluginContext pluginContext = new PipelinePluginContext(input, metrics, phaseSpec.isStageLoggingEnabled(),
                                                            phaseSpec.isProcessTimingEnabled());

    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(input.getToken(), input.getRuntimeArguments(),
                                                              input.getLogicalStartTime(), input, input.getNamespace());

    try {
      Condition condition = pluginContext.newPluginInstance(stageSpec.getName(), macroEvaluator);
      PipelineRuntime pipelineRuntime = new PipelineRuntime(input, metrics);
      ConditionContext conditionContext = new BasicConditionContext(input, pipelineRuntime, stageSpec);
      boolean result = condition.apply(conditionContext);
      WorkflowToken token = input.getToken();
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
