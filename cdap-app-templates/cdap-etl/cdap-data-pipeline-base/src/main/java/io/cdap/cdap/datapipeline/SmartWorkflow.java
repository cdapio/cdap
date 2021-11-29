/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.schedule.ProgramStatusTriggerInfo;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.api.workflow.NodeValue;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.AlertPublisherContext;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.condition.Condition;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.batch.ActionSpec;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.batch.BatchPipelineSpec;
import io.cdap.cdap.etl.batch.BatchPipelineSpecGenerator;
import io.cdap.cdap.etl.batch.WorkflowBackedActionContext;
import io.cdap.cdap.etl.batch.condition.PipelineCondition;
import io.cdap.cdap.etl.batch.connector.AlertPublisherSink;
import io.cdap.cdap.etl.batch.connector.AlertReader;
import io.cdap.cdap.etl.batch.connector.ConnectorSource;
import io.cdap.cdap.etl.batch.connector.MultiConnectorSource;
import io.cdap.cdap.etl.batch.customaction.PipelineAction;
import io.cdap.cdap.etl.batch.mapreduce.ETLMapReduce;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAlertPublisherContext;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.FieldOperationTypeAdapter;
import io.cdap.cdap.etl.common.LocationAwareMDCWrapperLogger;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.TrackedIterator;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.lineage.FieldLineageProcessor;
import io.cdap.cdap.etl.planner.ConditionBranches;
import io.cdap.cdap.etl.planner.ControlDag;
import io.cdap.cdap.etl.planner.Dag;
import io.cdap.cdap.etl.planner.DisjointConnectionsException;
import io.cdap.cdap.etl.planner.PipelinePlan;
import io.cdap.cdap.etl.planner.PipelinePlanner;
import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.v2.ArgumentMapping;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.PluginPropertyMapping;
import io.cdap.cdap.etl.proto.v2.TriggeringPropertyMapping;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.batch.ETLSpark;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data Pipeline Smart Workflow.
 */
public class SmartWorkflow extends AbstractWorkflow {
  public static final String NAME = "DataPipelineWorkflow";
  static final String TRIGGERING_PROPERTIES_MAPPING = "triggering.properties.mapping";

  private static final String RESOLVED_PLUGIN_PROPERTIES_MAP = "resolved.plugin.properties.map";

  private static final Logger LOG = LoggerFactory.getLogger(SmartWorkflow.class);
  private static final Logger WRAPPERLOGGER = new LocationAwareMDCWrapperLogger(LOG, Constants.EVENT_TYPE_TAG,
                                                                                Constants.PIPELINE_LIFECYCLE_TAG_VALUE);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter()).create();
  private static final Type STAGE_DATASET_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type STAGE_PROPERTIES_MAP = new TypeToken<Map<String, Map<String, String>>>() { }.getType();
  private static final Type STAGE_OPERATIONS_MAP = new TypeToken<Map<String, List<FieldOperation>>>() { }.getType();

  private final ApplicationConfigurer applicationConfigurer;
  private final Set<String> supportedPluginTypes;
  // connector stage -> local dataset name
  private final Map<String, String> connectorDatasets;
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
  private ETLBatchConfig config;
  private BatchPipelineSpec spec;
  private int connectorNum = 0;
  private int publisherNum = 0;

  public SmartWorkflow(ETLBatchConfig config, Set<String> supportedPluginTypes,
                       ApplicationConfigurer applicationConfigurer) {
    this.config = config;
    this.supportedPluginTypes = supportedPluginTypes;
    this.applicationConfigurer = applicationConfigurer;
    this.phaseNum = 1;
    this.connectorDatasets = new HashMap<>();
  }

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("Data Pipeline Workflow");

    // This will also register all the plugin in the workflow so that CDAP knows what plugins the
    // workflow needs to run. If a plugin has a requirement that will not be available during that run, CDAP can fail
    // the run early, before provisioning is performed.
    // If plugins were registered only at the application level, CDAP would not be able to fail the run early.
    try {
      spec = new BatchPipelineSpecGenerator(applicationConfigurer.getDeployedNamespace(), getConfigurer(),
                                            applicationConfigurer.getRuntimeConfigurer(),
                                            ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                            ImmutableSet.of(BatchSink.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE,
                                                            AlertPublisher.PLUGIN_TYPE),
                                            config.getEngine()).generateSpec(config);
    } catch (ValidationException e) {
      throw new IllegalArgumentException(
        String.format("Failed to configure pipeline: %s",
                      e.getFailures().isEmpty() ? e.getMessage() :
                        e.getFailures().iterator().next().getFullMessage()), e);
    }

    // append "_" to the connection name so it will not conflict with the system tag we add
    Set<String> connectionsUsed = spec.getConnectionsUsed().stream().map(s -> "_" + s).collect(Collectors.toSet());
    applicationConfigurer.emitMetadata(new Metadata(Collections.emptyMap(), connectionsUsed), MetadataScope.SYSTEM);

    stageSpecs = new HashMap<>();
    useSpark = config.getEngine() == Engine.SPARK;
    for (StageSpec stageSpec : spec.getStages()) {
      stageSpecs.put(stageSpec.getName(), stageSpec);
      String pluginType = stageSpec.getPlugin().getType();
      if (SparkCompute.PLUGIN_TYPE.equals(pluginType) || SparkSink.PLUGIN_TYPE.equals(pluginType)) {
        useSpark = true;
      }
    }

    plan = createPlan();

    WorkflowProgramAdder programAdder = new TrunkProgramAdder(getConfigurer());
    // single phase, just add the program directly
    if (plan.getPhases().size() == 1) {
      addProgram(plan.getPhases().keySet().iterator().next(), programAdder);
      setWorkflowProperties();
      return;
    }

    // Dag classes don't allow a 'dag' without connections
    if (plan.getPhaseConnections().isEmpty()) {

      WorkflowProgramAdder fork = programAdder.fork();
      for (String phaseName : plan.getPhases().keySet()) {
        addProgram(phaseName, fork);
      }
      fork.join();
      setWorkflowProperties();
      return;
    }

    /*
       ControlDag is used to flatten the dag that represents connections between phases.
       Connections between phases represent a happens-before relationship, not the flow of data.
       As such, phases can be shifted around as long as every happens-before relationship is maintained.
       The exception is condition phases. Connection from a condition to another phase must be maintained as is.

       Flattening a ControlDag will transform a dag into a special fork-join dag by moving phases around.
       We therefore cannot blindly flatten the phase connections.
       However, we validated earlier that condition outputs have a special property, where every stage following a
       condition can only have a single input. This means we will never need to flatten anything after the first
       set of conditions. We will only have to flatten what comes before the first set of conditions.
     */
    dag = new ControlDag(plan.getPhaseConnections());
    boolean dummyNodeAdded = false;
    Map<String, ConditionBranches> conditionBranches = plan.getConditionPhaseBranches();
    if (conditionBranches.isEmpty()) {
      // after flattening, there is guaranteed to be just one source
      dag.flatten();
    } else if (!conditionBranches.keySet().containsAll(dag.getSources())) {
      // Continue only if the condition node is not the source of the dag, otherwise dag is already in the
      // required form
      Set<String> conditions = conditionBranches.keySet();
      // flatten only the part of the dag starting from sources and ending in conditions/sinks.
      Set<String> dagNodes = dag.accessibleFrom(dag.getSources(), Sets.union(dag.getSinks(), conditions));
      Set<String> dagNodesWithoutCondition = Sets.difference(dagNodes, conditions);

      Set<Connection> connections = new HashSet<>();
      Deque<String> bfs = new LinkedList<>();

      Set<String> sinks = new HashSet<>();

      // If its a single phase without condition then no need to flatten
      if (dagNodesWithoutCondition.size() < 2) {
        sinks.addAll(dagNodesWithoutCondition);
      } else {
        /*
           Create a subdag from dagNodesWithoutCondition.
           There are a couple situations where this is not immediately possible. For example:

             source1 --|
                       |--> condition -- ...
             source2 --|

           Here, dagNodesWithoutCondition = [source1, source2], which is an invalid dag. Similarly:

             source --> condition -- ...

           Here, dagNodesWithoutCondition = [source], which is also invalid. In order to ensure that we have a
           valid dag, we just insert a dummy node as the first node in the subdag, adding a connection from the
           dummy node to all the sources.
         */
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
      // need to make sure we don't call also() if this is the final branch
      Iterator<String> outputIter = dag.getNodeOutputs(dummyNode).iterator();
      addBranchPrograms(outputIter.next(), fork, false);
      while (outputIter.hasNext()) {
        fork = fork.also();
        addBranchPrograms(outputIter.next(), fork, !outputIter.hasNext());
      }
    } else {
      String start = dag.getSources().iterator().next();
      addPrograms(start, programAdder);
    }

    setWorkflowProperties();
  }

  private PipelinePlan createPlan() {
    PipelinePlanner planner;
    Set<String> actionTypes = ImmutableSet.of(Action.PLUGIN_TYPE, Constants.SPARK_PROGRAM_PLUGIN_TYPE);
    Set<String> multiPortTypes = ImmutableSet.of(SplitterTransform.PLUGIN_TYPE);
    if (useSpark) {
      // if the pipeline uses spark, we don't need to break the pipeline up into phases, we can just have
      // a single phase.
      planner = new PipelinePlanner(supportedPluginTypes, ImmutableSet.of(), ImmutableSet.of(),
                                    actionTypes, multiPortTypes);
    } else {
      planner = new PipelinePlanner(supportedPluginTypes,
                                    ImmutableSet.of(BatchAggregator.PLUGIN_TYPE, BatchJoiner.PLUGIN_TYPE),
                                    ImmutableSet.of(SparkCompute.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE),
                                    actionTypes, multiPortTypes);
    }
    return planner.plan(spec);
  }

  private void updateTokenWithTriggeringProperties(TriggeringScheduleInfo scheduleInfo,
                                                   TriggeringPropertyMapping propertiesMapping,
                                                   WorkflowToken token) {
    List<ProgramStatusTriggerInfo> programStatusTriggerInfos = new ArrayList<>();
    for (TriggerInfo info : scheduleInfo.getTriggerInfos()) {
      if (info instanceof ProgramStatusTriggerInfo) {
        programStatusTriggerInfos.add((ProgramStatusTriggerInfo) info);
      }
    }
    // If no ProgramStatusTriggerInfo, no need of override the existing runtimeArgs
    if (programStatusTriggerInfos.isEmpty()) {
      return;
    }
    // Currently only expecting one trigger in a schedule
    ProgramStatusTriggerInfo triggerInfo = programStatusTriggerInfos.get(0);
    BasicArguments triggeringArguments = new BasicArguments(triggerInfo.getWorkflowToken(),
                                                            triggerInfo.getRuntimeArguments());
    // Get the value of every triggering pipeline arguments specified in the propertiesMapping and update newRuntimeArgs
    List<ArgumentMapping> argumentMappings = propertiesMapping.getArguments();
    for (ArgumentMapping mapping : argumentMappings) {
      String sourceKey = mapping.getSource();
      if (sourceKey == null) {
        LOG.warn("The name of argument from the triggering pipeline cannot be null, " +
                   "skip this argument mapping: '{}'.", mapping);
        continue;
      }
      String value = triggeringArguments.get(sourceKey);
      if (value == null) {
        LOG.warn("Runtime argument '{}' is not found in run '{}' of the triggering pipeline '{}' " +
                   "in namespace '{}' ",
                 sourceKey, triggerInfo.getRunId(), triggerInfo.getApplicationName(),
                 triggerInfo.getNamespace());
        continue;
      }
      // Use the argument name in the triggering pipeline if target is not specified
      String targetKey = mapping.getTarget() == null ? sourceKey : mapping.getTarget();
      token.put(targetKey, value);
    }
    // Get the resolved plugin properties map from triggering pipeline's workflow token in triggeringArguments
    Map<String, Map<String, String>> resolvedProperties =
      GSON.fromJson(triggeringArguments.get(RESOLVED_PLUGIN_PROPERTIES_MAP), STAGE_PROPERTIES_MAP);
    for (PluginPropertyMapping mapping : propertiesMapping.getPluginProperties()) {
      String stageName = mapping.getStageName();
      if (stageName == null) {
        LOG.warn("The name of the stage cannot be null in plugin property mapping, skip this mapping: '{}'.", mapping);
        continue;
      }
      Map<String, String> pluginProperties = resolvedProperties.get(stageName);
      if (pluginProperties == null) {
        LOG.warn("No plugin properties can be found with stage name '{}' in triggering pipeline '{}' " +
                   "in namespace '{}' ", mapping.getStageName(), triggerInfo.getApplicationName(),
                 triggerInfo.getNamespace());
        continue;
      }
      String sourceKey = mapping.getSource();
      if (sourceKey == null) {
        LOG.warn("The name of argument from the triggering pipeline cannot be null, " +
                   "skip this argument mapping: '{}'.", mapping);
        continue;
      }
      String value = pluginProperties.get(sourceKey);
      if (value == null) {
        LOG.warn("No property with name '{}' can be found in plugin '{}' of the triggering pipeline '{}' " +
                   "in namespace '{}' ", sourceKey, stageName, triggerInfo.getApplicationName(),
                 triggerInfo.getNamespace());
        continue;
      }
      // Use the argument name in the triggering pipeline if target is not specified
      String targetKey = mapping.getTarget() == null ? sourceKey : mapping.getTarget();
      token.put(targetKey, value);
    }
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);
    context.enableFieldLineageConsolidation();
    TriggeringScheduleInfo scheduleInfo = context.getTriggeringScheduleInfo();
    if (scheduleInfo != null) {
      String propertiesMappingString = scheduleInfo.getProperties().get(TRIGGERING_PROPERTIES_MAPPING);
      if (propertiesMappingString != null) {
        TriggeringPropertyMapping propertiesMapping =
          GSON.fromJson(propertiesMappingString, TriggeringPropertyMapping.class);
        updateTokenWithTriggeringProperties(scheduleInfo, propertiesMapping, context.getToken());
      }
    }
    PipelineRuntime pipelineRuntime = new PipelineRuntime(context, workflowMetrics);
    WRAPPERLOGGER.info("Pipeline '{}' is started by user '{}' with arguments {}",
                       context.getApplicationSpecification().getName(),
                       UserGroupInformation.getCurrentUser().getShortUserName(),
                       pipelineRuntime.getArguments().asMap());

    alertPublishers = new HashMap<>();
    postActions = new LinkedHashMap<>();
    spec = GSON.fromJson(context.getWorkflowSpecification().getProperty(Constants.PIPELINE_SPEC_KEY),
                         BatchPipelineSpec.class);
    stageSpecs = new HashMap<>();
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                                                              context.getLogicalStartTime(), context,
                                                              context, context.getNamespace());
    PluginContext pluginContext = new PipelinePluginContext(context, workflowMetrics,
                                                            spec.isStageLoggingEnabled(),
                                                            spec.isProcessTimingEnabled());
    for (ActionSpec actionSpec : spec.getEndingActions()) {
      String stageName = actionSpec.getName();
      postActions.put(stageName, pluginContext.newPluginInstance(stageName, macroEvaluator));
      stageSpecs.put(stageName, StageSpec.builder(stageName, actionSpec.getPluginSpec())
        .setStageLoggingEnabled(spec.isStageLoggingEnabled())
        .setProcessTimingEnabled(spec.isProcessTimingEnabled())
        .setMaxPreviewRecords(spec.getNumOfRecordsPreview())
        .build());
    }

    for (StageSpec stageSpec : spec.getStages()) {
      String stageName = stageSpec.getName();
      stageSpecs.put(stageName, stageSpec);
      if (AlertPublisher.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
        AlertPublisher alertPublisher = context.newPluginInstance(stageName, macroEvaluator);
        alertPublishers.put(stageName, alertPublisher);
      }
    }

    WRAPPERLOGGER.info("Pipeline '{}' running", context.getApplicationSpecification().getName());
  }

  @Override
  public void destroy() {
    WorkflowContext workflowContext = getContext();
    PipelineRuntime pipelineRuntime = new PipelineRuntime(workflowContext, workflowMetrics);
    // Execute the post actions only if pipeline is not running in preview mode.
    if (!workflowContext.getDataTracer(PostAction.PLUGIN_TYPE).isEnabled()) {
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

    Map<String, String> connectorDatasets = GSON.fromJson(
      workflowContext.getWorkflowSpecification().getProperty(Constants.CONNECTOR_DATASETS), STAGE_DATASET_MAP);
    // publish all alerts
    for (Map.Entry<String, AlertPublisher> alertPublisherEntry : alertPublishers.entrySet()) {
      String stageName = alertPublisherEntry.getKey();
      AlertPublisher alertPublisher = alertPublisherEntry.getValue();
      FileSet alertConnector = workflowContext.getDataset(connectorDatasets.get(stageName));
      try (CloseableIterator<Alert> alerts = new AlertReader(alertConnector)) {
        if (!alerts.hasNext()) {
          continue;
        }

        StageMetrics stageMetrics = new DefaultStageMetrics(workflowMetrics, stageName);
        StageSpec stageSpec = stageSpecs.get(stageName);
        AlertPublisherContext alertContext =
          new DefaultAlertPublisherContext(pipelineRuntime, stageSpec, workflowContext, workflowContext.getAdmin());
        alertPublisher.initialize(alertContext);

        TrackedIterator<Alert> trackedIterator =
          new TrackedIterator<>(alerts, stageMetrics, Constants.Metrics.RECORDS_IN);
        alertPublisher.publish(trackedIterator);
      } catch (Exception e) {
        LOG.warn("Stage {} had errors publishing alerts. Alerts may not have been published.", stageName, e);
      } finally {
        try {
          alertPublisher.destroy();
        } catch (Exception e) {
          LOG.warn("Error destroying alert publisher for stage {}", stageName, e);
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

    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                                                              workflowContext.getLogicalStartTime(),
                                                              workflowContext, workflowContext,
                                                              workflowContext.getNamespace());
    // Get resolved plugin properties
    Map<String, Map<String, String>> resolvedProperties = new HashMap<>();
    for (StageSpec spec : stageSpecs.values()) {
      String stageName = spec.getName();
      resolvedProperties.put(stageName, workflowContext.getPluginProperties(stageName, macroEvaluator).getProperties());
    }
    // Add resolved plugin properties to workflow token as a JSON String
    workflowContext.getToken().put(RESOLVED_PLUGIN_PROPERTIES_MAP, GSON.toJson(resolvedProperties));

    // record only if the Workflow is successful
    if (status != ProgramStatus.COMPLETED) {
      return;
    }

    // Collect field operations from each phase
    WorkflowToken token = workflowContext.getToken();
    List<NodeValue> allNodeValues = token.getAll(Constants.FIELD_OPERATION_KEY_IN_WORKFLOW_TOKEN);

    if (allNodeValues.isEmpty()) {
      // no field lineage recorded by any stage
      return;
    }

    Map<String, List<FieldOperation>> allStageOperations = new HashMap<>();
    for (StageSpec stageSpec : stageSpecs.values()) {
      allStageOperations.put(stageSpec.getName(), new ArrayList<>());
    }
    for (NodeValue nodeValue : allNodeValues) {
      Map<String, List<FieldOperation>> stageOperations
        = GSON.fromJson(nodeValue.getValue().toString(), STAGE_OPERATIONS_MAP);
      for (Map.Entry<String, List<FieldOperation>> entry : stageOperations.entrySet()) {
        // allStageOperations only contains the stages from the stageSpecs, which does not include the connector stage,
        // but the stageOperations might contain connector stage if the pipeline has multiple phases, so this check
        // is needed to avoid an NPE, the connector stage always has an empty field level operations so we can just
        // ignore them
        if (allStageOperations.containsKey(entry.getKey())) {
          allStageOperations.get(entry.getKey()).addAll(entry.getValue());
        }
      }
    }

    FieldLineageProcessor processor = new FieldLineageProcessor(spec);
    Set<Operation> processedOperations = processor.validateAndConvert(allStageOperations);
    if (!processedOperations.isEmpty()) {
      workflowContext.record(processedOperations);
    }
  }

  private void addPrograms(String node, WorkflowProgramAdder programAdder) {
    programAdder = addProgram(node, programAdder);
    Iterator<String> outputIter = dag.getNodeOutputs(node).iterator();
    if (!outputIter.hasNext()) {
      return;
    }
    String output = outputIter.next();

    ConditionBranches branches = plan.getConditionPhaseBranches().get(node);
    if (branches != null) {
      // This is condition
      addConditionBranches(programAdder, branches);
    } else {
      // if this is a fork
      if (outputIter.hasNext()) {
        WorkflowProgramAdder fork = programAdder.fork();
        addBranchPrograms(output, fork, false);

        while (outputIter.hasNext()) {
          fork = fork.also();
          addBranchPrograms(outputIter.next(), fork, !outputIter.hasNext());
        }
      } else {
        addPrograms(output, programAdder);
      }
    }
  }

  private void addBranchPrograms(String node, WorkflowProgramAdder programAdder, boolean shouldJoin) {
    // if this is a join node
    if (dag.getNodeInputs(node).size() > 1) {
      // if we've reached the join from the final branch of the fork
      if (shouldJoin) {
        // join the fork and continue on
        addPrograms(node, programAdder.join());
      }
      return;
    }

    programAdder = addProgram(node, programAdder);
    ConditionBranches branches = plan.getConditionPhaseBranches().get(node);
    if (branches != null) {
      programAdder = addConditionBranches(programAdder, branches);
      if (shouldJoin) {
        programAdder.join();
      }
    } else {
      // if we're already on a branch, we should never have another branch for non-condition programs
      Set<String> nodeOutputs = dag.getNodeOutputs(node);
      if (nodeOutputs.size() > 1) {
        throw new IllegalStateException("Found an unexpected non-condition branch while on another branch. " +
                                          "This means there is a pipeline planning bug. " +
                                          "Please contact the CDAP team to open a bug report.");
      }
      if (!nodeOutputs.isEmpty()) {
        addBranchPrograms(dag.getNodeOutputs(node).iterator().next(), programAdder, shouldJoin);
      }
    }
  }

  private BatchPhaseSpec getPhaseSpec(String programName, PipelinePhase phase) {
    Map<String, String> phaseConnectorDatasets = new HashMap<>();
    // if this phase uses connectors, add the local dataset for that connector if we haven't already
    for (StageSpec connectorInfo : phase.getStagesOfType(Constants.Connector.PLUGIN_TYPE)) {
      String connectorName = connectorInfo.getName();
      if (!connectorDatasets.containsKey(connectorName)) {
        String datasetName = "conn-" + connectorNum++;
        connectorDatasets.put(connectorName, datasetName);
        phaseConnectorDatasets.put(connectorName, datasetName);
        // add the local dataset
        ConnectorSource connectorSource = new MultiConnectorSource(datasetName, null);
        connectorSource.configure(getConfigurer());
      } else {
        phaseConnectorDatasets.put(connectorName, connectorDatasets.get(connectorName));
      }
    }

    // create a local dataset to store alerts. At the end of the phase, the dataset will be scanned and alerts
    // published.
    for (StageSpec alertPublisherInfo : phase.getStagesOfType(AlertPublisher.PLUGIN_TYPE)) {
      String stageName = alertPublisherInfo.getName();
      if (!connectorDatasets.containsKey(stageName)) {
        String datasetName = "alerts-" + publisherNum++;
        connectorDatasets.put(alertPublisherInfo.getName(), datasetName);
        phaseConnectorDatasets.put(alertPublisherInfo.getName(), datasetName);
        AlertPublisherSink alertPublisherSink = new AlertPublisherSink(datasetName, null);
        alertPublisherSink.configure(getConfigurer());
      } else {
        phaseConnectorDatasets.put(stageName, connectorDatasets.get(stageName));
      }
    }

    return new BatchPhaseSpec(programName, phase, spec.getResources(), spec.getDriverResources(),
                              spec.getClientResources(), spec.isStageLoggingEnabled(), spec.isProcessTimingEnabled(),
                              phaseConnectorDatasets, spec.getNumOfRecordsPreview(), spec.getProperties(),
                              !plan.getConditionPhaseBranches().isEmpty(), spec.getSqlEngineStageSpec());
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
      programAdder = programAdder.condition(new PipelineCondition(batchPhaseSpec));
    } else if (pluginTypes.contains(Constants.SPARK_PROGRAM_PLUGIN_TYPE)) {
      // spark programs will be all by themselves in a phase
      String stageName = phase.getStagesOfType(Constants.SPARK_PROGRAM_PLUGIN_TYPE).iterator().next().getName();
      StageSpec stageSpec = stageSpecs.get(stageName);
      applicationConfigurer.addSpark(new ExternalSparkProgram(batchPhaseSpec, stageSpec,
                                                              applicationConfigurer.getRuntimeConfigurer(),
                                                              applicationConfigurer.getDeployedNamespace()));
      programAdder.addSpark(programName);
    } else if (useSpark) {
      applicationConfigurer.addSpark(new ETLSpark(batchPhaseSpec, applicationConfigurer.getRuntimeConfigurer(),
                                                  applicationConfigurer.getDeployedNamespace()));
      programAdder.addSpark(programName);
    } else {
      applicationConfigurer.addMapReduce(new ETLMapReduce(batchPhaseSpec,
                                                          new HashSet<>(connectorDatasets.values()),
                                                          applicationConfigurer.getRuntimeConfigurer(),
                                                          applicationConfigurer.getDeployedNamespace()));
      programAdder.addMapReduce(programName);
    }
    return programAdder;
  }

  private WorkflowProgramAdder addConditionBranches(WorkflowProgramAdder conditionAdder, ConditionBranches branches) {
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
    return conditionAdder.end();
  }

  private void setWorkflowProperties() {
    Map<String, String> properties = new HashMap<>();
    // set the pipeline spec as a property in case somebody like the UI wants to read it
    properties.put(Constants.PIPELINE_SPEC_KEY, GSON.toJson(spec));
    // set the connector dataset as a property to be able to figure out the mapping of the stage name of the alert
    // publisher to the local datasets for it, so that we can publish alerts in destroy()
    properties.put(Constants.CONNECTOR_DATASETS, GSON.toJson(connectorDatasets));
    setProperties(properties);
  }
}
