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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.AlertPublisherContext;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.batch.ActionSpec;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.BatchPipelineSpec;
import co.cask.cdap.etl.batch.WorkflowBackedActionContext;
import co.cask.cdap.etl.batch.condition.PipelineCondition;
import co.cask.cdap.etl.batch.connector.AlertPublisherSink;
import co.cask.cdap.etl.batch.connector.AlertReader;
import co.cask.cdap.etl.batch.connector.ConnectorSource;
import co.cask.cdap.etl.batch.connector.MultiConnectorSource;
import co.cask.cdap.etl.batch.customaction.PipelineAction;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultAlertPublisherContext;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.TrackedIterator;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.planner.ConditionBranches;
import co.cask.cdap.etl.planner.ControlDag;
import co.cask.cdap.etl.planner.Dag;
import co.cask.cdap.etl.planner.DisjointConnectionsException;
import co.cask.cdap.etl.planner.PipelinePlan;
import co.cask.cdap.etl.planner.PipelinePlanner;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spark.batch.ETLSpark;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
  private Map<String, AlertPublisher> alertPublishers;
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
    for (StageSpec stageSpec : spec.getStages()) {
      stageSpecs.put(stageSpec.getName(), stageSpec);
      String pluginType = stageSpec.getPlugin().getType();
      if (SparkCompute.PLUGIN_TYPE.equals(pluginType) || SparkSink.PLUGIN_TYPE.equals(pluginType)) {
        useSpark = true;
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

    WorkflowProgramAdder programAdder = new TrunkProgramAdder(getConfigurer());
    // single phase, just add the program directly
    if (plan.getPhases().size() == 1) {
      addProgram(plan.getPhases().keySet().iterator().next(), programAdder);
      return;
    }

    // Dag classes don't allow a 'dag' without connections
    if (plan.getPhaseConnections().isEmpty()) {

      WorkflowProgramAdder fork = programAdder.fork();
      for (String phaseName : plan.getPhases().keySet()) {
        addProgram(phaseName, fork);
      }
      fork.join();
      return;
    }

    dag = new ControlDag(plan.getPhaseConnections());
    boolean dummyNodeAdded = false;
    Map<String, ConditionBranches> conditionBranches = plan.getConditionPhaseBranches();
    if (conditionBranches.isEmpty()) {
      // after flattening, there is guaranteed to be just one source
      dag.flatten();
    } else if (!conditionBranches.keySet().containsAll(dag.getSources())) {
      // Continue only if the conditon node is not the source of the dag, otherwise dag is already in the
      // required form
      Set<String> conditions = conditionBranches.keySet();
      // flatten only the part of the dag starting from sources and ending in conditions/sinks.
      Set<String> dagNodes = dag.accessibleFrom(dag.getSources(), Sets.union(dag.getSinks(), conditions));
      Set<String> dagNodesWithoutCondition = Sets.difference(dagNodes, conditions);

      Set<Connection> connections = new HashSet<>();
      Deque<String> bfs = new LinkedList<>();

      Set<String> sinks = new HashSet<>();

      // If its a single phase without condition then no need to flatten
      if (dagNodesWithoutCondition.size() > 1) {
        Dag subDag;
        try {
          subDag = dag.createSubDag(dagNodesWithoutCondition);
        } catch (IllegalArgumentException | DisjointConnectionsException e) {
          // DisjointConnectionsException thrown when islands are created from the dagNodesWithoutCondition
          // IllegalArgumentException thrown when connections are empty
          // In both cases we need to add dummy node and create connected Dag
          String dummyNode = "dummy";
          dummyNodeAdded = true;
          Set<Connection> subDagConnections = new HashSet<>();
          for (String source : dag.getSources()) {
            subDagConnections.add(new Connection(dummyNode, source));
          }
          Deque<String> subDagBFS = new LinkedList<>();
          subDagBFS.addAll(dag.getSources());

          while (subDagBFS.peek() != null) {
            String node = subDagBFS.poll();
            for (String output : dag.getNodeOutputs(node)) {
              if (dagNodesWithoutCondition.contains(output)) {
                subDagConnections.add(new Connection(node, output));
                subDagBFS.add(output);
              }
            }
          }
          subDag = new Dag(subDagConnections);
        }

        ControlDag cdag = new ControlDag(subDag);
        cdag.flatten();

        // Add all connections from cdag
        bfs.addAll(cdag.getSources());
        while (bfs.peek() != null) {
          String node = bfs.poll();
          for (String output : cdag.getNodeOutputs(node)) {
            connections.add(new Connection(node, output));
            bfs.add(output);
          }
        }
        sinks.addAll(cdag.getSinks());
      } else {
        sinks.addAll(dagNodesWithoutCondition);
      }

      // Add back the existing condition nodes and corresponding conditions
      Set<String> conditionsFromDag = Sets.intersection(dagNodes, conditions);
      for (String condition : conditionsFromDag) {
        connections.add(new Connection(sinks.iterator().next(), condition));
      }
      bfs.addAll(Sets.intersection(dagNodes, conditions));
      while (bfs.peek() != null) {
        String node = bfs.poll();
        ConditionBranches branches = conditionBranches.get(node);
        if (branches == null) {
          // not a condition node. add outputs
          for (String output : dag.getNodeOutputs(node)) {
            connections.add(new Connection(node, output));
            bfs.add(output);
          }
        } else {
          // condition node
          for (Boolean condition : Arrays.asList(true, false)) {
            String phase = condition ? branches.getTrueOutput() : branches.getFalseOutput();
            if (phase == null) {
              continue;
            }
            connections.add(new Connection(node, phase, condition));
            bfs.add(phase);
          }
        }
      }
      dag = new ControlDag(connections);
    }

    if (dummyNodeAdded) {
      WorkflowProgramAdder fork = programAdder.fork();
      String dummyNode = dag.getSources().iterator().next();
      for (String output : dag.getNodeOutputs(dummyNode)) {
        // need to make sure we don't call also() if this is the final branch
        if (!addBranchPrograms(output, fork)) {
          fork = fork.also();
        }
      }
    } else {
      String start = dag.getSources().iterator().next();
      addPrograms(start, programAdder);
    }
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);

    String arguments = Joiner.on(", ").withKeyValueSeparator("=").join(context.getRuntimeArguments());
    WRAPPERLOGGER.info("Pipeline '{}' is started by user '{}' with arguments {}",
                       context.getApplicationSpecification().getName(),
                       UserGroupInformation.getCurrentUser().getShortUserName(),
                       arguments);

    alertPublishers = new HashMap<>();
    postActions = new LinkedHashMap<>();
    spec = GSON.fromJson(context.getWorkflowSpecification().getProperty(Constants.PIPELINE_SPEC_KEY),
                         BatchPipelineSpec.class);
    stageSpecs = new HashMap<>();
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(context.getToken(), context.getRuntimeArguments(),
                                                              context.getLogicalStartTime(), context,
                                                              context.getNamespace());
    PipelineRuntime pipelineRuntime = new PipelineRuntime(context, workflowMetrics);
    PluginContext pluginContext = new PipelinePluginContext(context, workflowMetrics,
                                                            spec.isStageLoggingEnabled(),
                                                            spec.isProcessTimingEnabled());
    for (ActionSpec actionSpec : spec.getEndingActions()) {
      String stageName = actionSpec.getName();
      postActions.put(stageName, (PostAction) pluginContext.newPluginInstance(stageName, macroEvaluator));
      stageSpecs.put(stageName, StageSpec.builder(stageName, actionSpec.getPluginSpec())
        .setStageLoggingEnabled(spec.isStageLoggingEnabled())
        .setProcessTimingEnabled(spec.isProcessTimingEnabled())
        .build());
    }

    for (StageSpec stageSpec : spec.getStages()) {
      String stageName = stageSpec.getName();
      stageSpecs.put(stageName, stageSpec);
      if (AlertPublisher.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
        AlertPublisher alertPublisher = context.newPluginInstance(stageName, macroEvaluator);
        AlertPublisherContext alertContext =
          new DefaultAlertPublisherContext(pipelineRuntime, stageSpec, context, context.getAdmin());
        alertPublisher.initialize(alertContext);
        alertPublishers.put(stageName, alertPublisher);
      }
    }

    WRAPPERLOGGER.info("Pipeline '{}' running", context.getApplicationSpecification().getName());
  }

  @Override
  public void destroy() {
    WorkflowContext workflowContext = getContext();

    // Execute the post actions only if pipeline is not running in preview mode.
    if (!workflowContext.getDataTracer(PostAction.PLUGIN_TYPE).isEnabled()) {
      PipelineRuntime pipelineRuntime = new PipelineRuntime(workflowContext, workflowMetrics);
      for (Map.Entry<String, PostAction> endingActionEntry : postActions.entrySet()) {
        String name = endingActionEntry.getKey();
        PostAction action = endingActionEntry.getValue();
        StageSpec stageSpec = stageSpecs.get(name);
        BatchActionContext context = new WorkflowBackedActionContext(workflowContext, pipelineRuntime, stageSpec);
        try {
          action.run(context);
        } catch (Throwable t) {
          LOG.error("Error while running post action {}.", name, t);
        }
      }

    }

    // publish all alerts
    for (Map.Entry<String, AlertPublisher> alertPublisherEntry : alertPublishers.entrySet()) {
      String name = alertPublisherEntry.getKey();
      AlertPublisher alertPublisher = alertPublisherEntry.getValue();
      PartitionedFileSet alertConnector = workflowContext.getDataset(name);
      try (CloseableIterator<Alert> alerts =
             new AlertReader(alertConnector.getPartitions(PartitionFilter.ALWAYS_MATCH))) {
        StageMetrics stageMetrics = new DefaultStageMetrics(workflowMetrics, name);
        TrackedIterator<Alert> trackedIterator =
          new TrackedIterator<>(alerts, stageMetrics, Constants.Metrics.RECORDS_IN);
        alertPublisher.publish(trackedIterator);
      } catch (Exception e) {
        LOG.warn("Stage {} had errors publishing alerts. Alerts may not have been published.", name, e);
      } finally {
        try {
          alertPublisher.destroy();
        } catch (Exception e) {
          LOG.warn("Error destroying alert publisher for stage {}", name, e);
        }
      }
    }

    ProgramStatus status = getContext().getState().getStatus();
    if (status == ProgramStatus.FAILED) {
      WRAPPERLOGGER.error("Pipeline '{}' failed.", getContext().getApplicationSpecification().getName());
    } else {
      WRAPPERLOGGER.info("Pipeline '{}' {}.", getContext().getApplicationSpecification().getName(),
                         status == ProgramStatus.COMPLETED ? "succeeded" : status.name().toLowerCase());
    }
  }

  private void addPrograms(String node, WorkflowProgramAdder programAdder) {
    programAdder = addProgram(node, programAdder);
    Set<String> outputs = dag.getNodeOutputs(node);
    if (outputs.isEmpty()) {
      return;
    }

    ConditionBranches branches = plan.getConditionPhaseBranches().get(node);
    if (branches != null) {
      // This is condition
      addCondition(programAdder, branches);
    } else {
      // if this is a fork
      if (outputs.size() > 1) {
        WorkflowProgramAdder fork = programAdder.fork();
        for (String output : outputs) {
          // need to make sure we don't call also() if this is the final branch
          if (!addBranchPrograms(output, fork)) {
            fork = fork.also();
          }
        }
      } else {
        addPrograms(outputs.iterator().next(), programAdder);
      }
    }
  }

  // returns whether this is the final branch of the fork
  private boolean addBranchPrograms(String node, WorkflowProgramAdder programAdder) {
    // if this is a join node
    Set<String> inputs = dag.getNodeInputs(node);
    if (dag.getNodeInputs(node).size() > 1) {
      // if we've reached the join from the final branch of the fork
      if (dag.visit(node) == inputs.size()) {
        // join the fork and continue on
        addPrograms(node, programAdder.join());
        return true;
      } else {
        return false;
      }
    }

    // a flattened control dag guarantees that if this is not a join node, there is exactly one output
    addProgram(node, programAdder);
    return addBranchPrograms(dag.getNodeOutputs(node).iterator().next(), programAdder);
  }

  private BatchPhaseSpec getPhaseSpec(String programName, PipelinePhase phase) {
    // if this phase uses connectors, add the local dataset for that connector if we haven't already
    for (StageSpec connectorInfo : phase.getStagesOfType(Constants.Connector.PLUGIN_TYPE)) {
      String connectorName = connectorInfo.getName();
      String datasetName = connectorDatasets.get(connectorName);
      if (datasetName == null) {
        datasetName = "conn-" + connectorNum++;
        connectorDatasets.put(connectorName, datasetName);
        // add the local dataset
        ConnectorSource connectorSource = new MultiConnectorSource(datasetName, null);
        connectorSource.configure(getConfigurer());
      }
    }
    // create a local dataset to store alerts. At the end of the phase, the dataset will be scanned and alerts
    // published.
    for (StageSpec alertPublisherInfo : phase.getStagesOfType(AlertPublisher.PLUGIN_TYPE)) {
      String stageName = alertPublisherInfo.getName();
      AlertPublisherSink alertPublisherSink = new AlertPublisherSink(stageName, null);
      alertPublisherSink.configure(getConfigurer());
    }

    Map<String, String> phaseConnectorDatasets = new HashMap<>();
    for (StageSpec connectorStage : phase.getStagesOfType(Constants.Connector.PLUGIN_TYPE)) {
      phaseConnectorDatasets.put(connectorStage.getName(), connectorDatasets.get(connectorStage.getName()));
    }

    return new BatchPhaseSpec(programName, phase, spec.getResources(), spec.getDriverResources(),
                              spec.getClientResources(), spec.isStageLoggingEnabled(), spec.isProcessTimingEnabled(),
                              phaseConnectorDatasets, spec.getNumOfRecordsPreview(), spec.getProperties());
  }

  private WorkflowProgramAdder addProgram(String phaseName, WorkflowProgramAdder programAdder) {
    PipelinePhase phase = plan.getPhase(phaseName);
    // if the origin plan didn't have this name, it means it is a join node
    // artificially added by the control dag flattening process. So nothing to add, skip it
    if (phase == null) {
      return programAdder;
    }

    // can't use phase name as a program name because it might contain invalid characters
    String programName = "phase-" + phaseNum;
    phaseNum++;

    BatchPhaseSpec batchPhaseSpec = getPhaseSpec(programName, phase);

    Set<String> pluginTypes = batchPhaseSpec.getPhase().getPluginTypes();
    if (pluginTypes.contains(Action.PLUGIN_TYPE)) {
      // actions will be all by themselves in a phase
      programAdder.addAction(new PipelineAction(batchPhaseSpec));
    } else if (pluginTypes.contains(Condition.PLUGIN_TYPE)) {
      // conditions will be all by themselves in a phase
      // addCondition(programAdder, phaseName, batchPhaseSpec);
      programAdder = programAdder.condition(new PipelineCondition(batchPhaseSpec));
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
      applicationConfigurer.addMapReduce(new ETLMapReduce(batchPhaseSpec,
                                                          new HashSet<>(connectorDatasets.values())));
      programAdder.addMapReduce(programName);
    }
    return programAdder;
  }

  private void addCondition(WorkflowProgramAdder conditionAdder, ConditionBranches branches) {
    // Add all phases on the true branch here
    String trueOutput = branches.getTrueOutput();
    if (trueOutput != null) {
      addPrograms(trueOutput, conditionAdder);
    }
    String falseOutput = branches.getFalseOutput();
    if (falseOutput != null) {
      conditionAdder.otherwise();
      addPrograms(falseOutput, conditionAdder);
    }
    conditionAdder.end();
  }
}
