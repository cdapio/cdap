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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.spark.batch.ETLSpark;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Workflow for scheduling Batch ETL MapReduce Driver.
 */
public class ETLWorkflow extends AbstractWorkflow {

  public static final String NAME = "ETLWorkflow";
  public static final String DESCRIPTION = "Workflow for ETL Batch MapReduce Driver";
  private static final Logger LOG = LoggerFactory.getLogger(ETLWorkflow.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private final Engine engine;
  private final BatchPipelineSpec spec;
  private Map<String, PostAction> postActions;
  private Map<String, StageSpec> postActionSpecs;

  // injected by cdap
  @SuppressWarnings("unused")
  private Metrics workflowMetrics;

  public ETLWorkflow(BatchPipelineSpec spec, Engine engine) {
    this.engine = engine;
    this.spec = spec;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setDescription(DESCRIPTION);
    switch (engine) {
      case MAPREDUCE:
        addMapReduce(ETLMapReduce.NAME);
        break;
      case SPARK:
        addSpark(ETLSpark.class.getSimpleName());
        break;
    }
    Map<String, String> properties = new HashMap<>();
    properties.put("pipeline.spec", GSON.toJson(spec));
    setProperties(properties);
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);
    postActions = new LinkedHashMap<>();
    BatchPipelineSpec batchPipelineSpec =
      GSON.fromJson(context.getWorkflowSpecification().getProperty("pipeline.spec"), BatchPipelineSpec.class);
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(context.getToken(), context.getRuntimeArguments(),
                                                              context.getLogicalStartTime(), context,
                                                              context.getNamespace());
    postActionSpecs = new HashMap<>();
    for (ActionSpec actionSpec : batchPipelineSpec.getEndingActions()) {
      String name = actionSpec.getName();
      postActions.put(name, (PostAction) context.newPluginInstance(name, macroEvaluator));
      postActionSpecs.put(name, StageSpec.builder(name, actionSpec.getPluginSpec())
        .setProcessTimingEnabled(batchPipelineSpec.isProcessTimingEnabled())
        .setStageLoggingEnabled(batchPipelineSpec.isStageLoggingEnabled())
        .build());
    }
  }

  @Override
  public void destroy() {
    WorkflowContext workflowContext = getContext();
    PipelineRuntime pipelineRuntime = new PipelineRuntime(workflowContext, workflowMetrics);
    if (workflowContext.getDataTracer(PostAction.PLUGIN_TYPE).isEnabled()) {
      return;
    }
    for (Map.Entry<String, PostAction> endingActionEntry : postActions.entrySet()) {
      String name = endingActionEntry.getKey();
      PostAction action = endingActionEntry.getValue();
      StageSpec stageSpec = postActionSpecs.get(name);
      BatchActionContext context = new WorkflowBackedActionContext(workflowContext, pipelineRuntime, stageSpec);
      try {
        action.run(context);
      } catch (Throwable t) {
        LOG.error("Error while running ending action {}.", name, t);
      }
    }
  }
}
