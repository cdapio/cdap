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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.batch.ActionSpec;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.BatchPipelineSpec;
import co.cask.cdap.etl.batch.WorkflowBackedActionContext;
import co.cask.cdap.etl.batch.connector.ConnectorSource;
import co.cask.cdap.etl.batch.customaction.PipelineAction;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.planner.ControlDag;
import co.cask.cdap.etl.planner.PipelinePlan;
import co.cask.cdap.etl.planner.PipelinePlanner;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.batch.ETLSpark;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Data Pipeline Smart Workflow.
 */
public class SmartWorkflow extends AbstractWorkflow {
  public static final String NAME = "DataPipelineWorkflow";
  public static final String DESCRIPTION = "Data Pipeline Workflow";
  private static final Logger LOG = LoggerFactory.getLogger(SmartWorkflow.class);
  private static final Logger WRAPPERLOGGER = new LocationAwareMDCWrapperLogger(LOG, Constants.EVENT_TYPE_TAG,
                                                                                Constants.PIPELINE_LIFECYCLE_TAG_VALUE);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private final ApplicationConfigurer applicationConfigurer;
  private final Set<String> supportedPluginTypes;
  // connector stage -> local dataset name
  private final Map<String, String> connectorDatasets;
  private final Engine engine;
  private boolean useSpark;
  private PipelinePlan plan;
  private ControlDag dag;
  private int phaseNum;
  private Map<String, PostAction> postActions;
  private Map<String, StageSpec> stageSpecs;

  // injected by cdap
  @SuppressWarnings("unused")
  private Metrics workflowMetrics;

  private BatchPipelineSpec spec;
  private int connectorNum = 0;

  public SmartWorkflow(BatchPipelineSpec spec,
                       Set<String> supportedPluginTypes,
                       ApplicationConfigurer applicationConfigurer,
                       Engine engine) {
    this.spec = spec;
    this.supportedPluginTypes = supportedPluginTypes;
    this.applicationConfigurer = applicationConfigurer;
    this.phaseNum = 1;
    this.connectorDatasets = new HashMap<>();
    this.engine = engine;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setDescription(DESCRIPTION);

    // set the pipeline spec as a property in case somebody like the UI wants to read it
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINE_SPEC_KEY, GSON.toJson(spec));
    setProperties(properties);

    stageSpecs = new HashMap<>();
    useSpark = engine == Engine.SPARK;
    if (!useSpark) {
      for (StageSpec stageSpec : spec.getStages()) {
        stageSpecs.put(stageSpec.getName(), stageSpec);
        String pluginType = stageSpec.getPlugin().getType();
        if (SparkCompute.PLUGIN_TYPE.equals(pluginType) || SparkSink.PLUGIN_TYPE.equals(pluginType)) {
          useSpark = true;
          break;
        }
      }
    }

    PipelinePlanner planner;
    Set<String> actionTypes = ImmutableSet.of(Action.PLUGIN_TYPE, Constants.SPARK_PROGRAM_PLUGIN_TYPE);
    if (useSpark) {
      // if the pipeline uses spark, we don't need to break the pipeline up into phases, we can just have
      // a single phase.
      planner = new PipelinePlanner(supportedPluginTypes, ImmutableSet.<String>of(), ImmutableSet.<String>of(),
                                    actionTypes);
    } else {
      planner = new PipelinePlanner(supportedPluginTypes,
                                    ImmutableSet.of(BatchAggregator.PLUGIN_TYPE, BatchJoiner.PLUGIN_TYPE),
                                    ImmutableSet.of(SparkCompute.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE),
                                    actionTypes);
    }
    plan = planner.plan(spec);

    // single phase, just add the program directly
    if (plan.getPhases().size() == 1) {
      addProgram(plan.getPhases().keySet().iterator().next(), new TrunkProgramAdder(getConfigurer()));
      return;
    }

    // Dag classes don't allow a 'dag' without connections
    if (plan.getPhaseConnections().isEmpty()) {

      WorkflowProgramAdder programAdder;
      // multiple phases, do a fork then join
      WorkflowForkConfigurer forkConfigurer = getConfigurer().fork();
      programAdder = new BranchProgramAdder(forkConfigurer);
      for (String phaseName : plan.getPhases().keySet()) {
        addProgram(phaseName, programAdder);
      }
      if (forkConfigurer != null) {
        forkConfigurer.join();
      }
      return;
    }

    dag = new ControlDag(plan.getPhaseConnections());
    // after flattening, there is guaranteed to be just one source
    dag.flatten();
    String start = dag.getSources().iterator().next();
    addPrograms(start, getConfigurer());
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);

    String arguments = Joiner.on(", ").withKeyValueSeparator("=").join(context.getRuntimeArguments());
    WRAPPERLOGGER.info("Pipeline '{}' is started by user '{}' with arguments {}",
                       context.getApplicationSpecification().getName(),
                       UserGroupInformation.getCurrentUser().getShortUserName(),
                       arguments);

    postActions = new LinkedHashMap<>();
    spec = GSON.fromJson(context.getWorkflowSpecification().getProperty(Constants.PIPELINE_SPEC_KEY),
                         BatchPipelineSpec.class);
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(context.getToken(), context.getRuntimeArguments(),
                                                              context.getLogicalStartTime(), context,
                                                              context.getNamespace());
    PluginContext pluginContext = new PipelinePluginContext(context, workflowMetrics,
                                                            spec.isStageLoggingEnabled(),
                                                            spec.isProcessTimingEnabled());
    for (ActionSpec actionSpec : spec.getEndingActions()) {
      postActions.put(actionSpec.getName(), (PostAction) pluginContext.newPluginInstance(actionSpec.getName(),
                                                                                         macroEvaluator));
    }

    WRAPPERLOGGER.info("Pipeline '{}' running", context.getApplicationSpecification().getName());
  }

  @Override
  public void destroy() {
    WorkflowContext workflowContext = getContext();

    // Execute the post actions only if pipeline is not running in preview mode.
    if (!workflowContext.getDataTracer(PostAction.PLUGIN_TYPE).isEnabled()) {
      BasicArguments arguments = new BasicArguments(workflowContext.getToken(), workflowContext.getRuntimeArguments());
      for (Map.Entry<String, PostAction> endingActionEntry : postActions.entrySet()) {
        String name = endingActionEntry.getKey();
        PostAction action = endingActionEntry.getValue();
        StageInfo stageInfo = StageInfo.builder(name, PostAction.PLUGIN_TYPE)
          .setStageLoggingEnabled(spec.isStageLoggingEnabled())
          .setProcessTimingEnabled(spec.isProcessTimingEnabled())
          .build();
        BatchActionContext context = new WorkflowBackedActionContext(workflowContext, workflowMetrics,
                                                                     stageInfo, arguments);
        try {
          action.run(context);
        } catch (Throwable t) {
          LOG.error("Error while running post action {}.", name, t);
        }
      }
    }

    ProgramStatus status = getContext().getState().getStatus();
    WRAPPERLOGGER.info("Pipeline '{}' {}", getContext().getApplicationSpecification().getName(),
                       status == ProgramStatus.COMPLETED ? "succeeded" : status.name().toLowerCase());
  }

  private void addPrograms(String node, WorkflowConfigurer configurer) {
    addProgram(node, new TrunkProgramAdder(configurer));

    Set<String> outputs = dag.getNodeOutputs(node);
    if (outputs.isEmpty()) {
      return;
    }

    // if this is a fork
    if (outputs.size() > 1) {
      WorkflowForkConfigurer<? extends WorkflowConfigurer> forkConfigurer = configurer.fork();
      for (String output : outputs) {
        // need to make sure we don't call also() if this is the final branch
        if (!addBranchPrograms(output, forkConfigurer)) {
          forkConfigurer = forkConfigurer.also();
        }
      }
    } else {
      addPrograms(outputs.iterator().next(), configurer);
    }
  }

  // returns whether this is the final branch of the fork
  private boolean addBranchPrograms(String node, WorkflowForkConfigurer<? extends WorkflowConfigurer> forkConfigurer) {
    // if this is a join node
    Set<String> inputs = dag.getNodeInputs(node);
    if (dag.getNodeInputs(node).size() > 1) {
      // if we've reached the join from the final branch of the fork
      if (dag.visit(node) == inputs.size()) {
        // join the fork and continue on
        addPrograms(node, forkConfigurer.join());
        return true;
      } else {
        return false;
      }
    }

    // a flattened control dag guarantees that if this is not a join node, there is exactly one output
    addProgram(node, new BranchProgramAdder(forkConfigurer));
    return addBranchPrograms(dag.getNodeOutputs(node).iterator().next(), forkConfigurer);
  }

  private void addProgram(String phaseName, WorkflowProgramAdder programAdder) {
    PipelinePhase phase = plan.getPhase(phaseName);
    // if the origin plan didn't have this name, it means it is a join node
    // artificially added by the control dag flattening process. So nothing to add, skip it
    if (phase == null) {
      return;
    }

    // can't use phase name as a program name because it might contain invalid characters
    String programName = "phase-" + phaseNum;
    phaseNum++;

    // if this phase uses connectors, add the local dataset for that connector if we haven't already
    for (StageInfo connectorInfo : phase.getStagesOfType(Constants.CONNECTOR_TYPE)) {
      String connectorName = connectorInfo.getName();
      String datasetName = connectorDatasets.get(connectorName);
      if (datasetName == null) {
        datasetName = "conn-" + connectorNum++;
        connectorDatasets.put(connectorName, datasetName);
        // add the local dataset
        ConnectorSource connectorSource = new ConnectorSource(datasetName, null);
        connectorSource.configure(getConfigurer());
      }
    }

    Map<String, String> phaseConnectorDatasets = new HashMap<>();
    for (StageInfo connectorStage : phase.getStagesOfType(Constants.CONNECTOR_TYPE)) {
      phaseConnectorDatasets.put(connectorStage.getName(), connectorDatasets.get(connectorStage.getName()));
    }
    BatchPhaseSpec batchPhaseSpec = new BatchPhaseSpec(programName, phase, spec.getResources(),
                                                       spec.getDriverResources(),
                                                       spec.getClientResources(),
                                                       spec.isStageLoggingEnabled(),
                                                       spec.isProcessTimingEnabled(),
                                                       phaseConnectorDatasets,
                                                       spec.getNumOfRecordsPreview(),
                                                       spec.getProperties());

    Set<String> pluginTypes = batchPhaseSpec.getPhase().getPluginTypes();
    if (pluginTypes.contains(Action.PLUGIN_TYPE)) {
      // actions will be all by themselves in a phase
      programAdder.addAction(new PipelineAction(batchPhaseSpec));
    } else if (pluginTypes.contains(Constants.SPARK_PROGRAM_PLUGIN_TYPE)) {
      // spark programs will be all by themselves in a phase
      String stageName = phase.getStagesOfType(Constants.SPARK_PROGRAM_PLUGIN_TYPE).iterator().next().getName();
      StageSpec stageSpec = stageSpecs.get(stageName);
      applicationConfigurer.addSpark(new ExternalSparkProgram(batchPhaseSpec, stageSpec));
      programAdder.addSpark(programName);
    } else if (useSpark) {
      applicationConfigurer.addSpark(new ETLSpark(batchPhaseSpec));
      programAdder.addSpark(programName);
    } else {
      applicationConfigurer.addMapReduce(new ETLMapReduce(batchPhaseSpec));
      programAdder.addMapReduce(programName);
    }
  }

}
