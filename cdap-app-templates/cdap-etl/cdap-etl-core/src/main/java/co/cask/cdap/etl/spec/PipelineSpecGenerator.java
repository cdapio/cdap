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

package co.cask.cdap.etl.spec;

import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.MultiInputPipelineConfigurable;
import co.cask.cdap.etl.api.MultiOutputPipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkJoiner;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.common.ArtifactSelectorProvider;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.etl.common.DefaultStageConfigurer;
import co.cask.cdap.etl.planner.Dag;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.v2.ETLConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This is run at application configure time to take an application config {@link ETLConfig} and call
 * {@link PipelineConfigurable#configurePipeline(PipelineConfigurer)} on all plugins in the pipeline.
 * This generates a {@link PipelineSpec} which the programs understand.
 *
 * @param <C> the type of user provided config
 * @param <P> the pipeline specification generated from the config
 * @param <T> the type of the platform configurer
 */
public abstract class PipelineSpecGenerator<C extends ETLConfig,
                                            P extends PipelineSpec,
                                            T extends PluginConfigurer & DatasetConfigurer> {
  private static final Set<String> VALID_ERROR_INPUTS = ImmutableSet.of(
    BatchSource.PLUGIN_TYPE, Transform.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE, ErrorTransform.PLUGIN_TYPE);
  protected final T configurer;
  protected final Engine engine;
  private final Set<String> sourcePluginTypes;
  private final Set<String> sinkPluginTypes;

  protected PipelineSpecGenerator(T configurer,
                                  Set<String> sourcePluginTypes,
                                  Set<String> sinkPluginTypes,
                                  Engine engine) {
    this.configurer = configurer;
    this.sourcePluginTypes = sourcePluginTypes;
    this.sinkPluginTypes = sinkPluginTypes;
    this.engine = engine;
  }

  /**
   * Validate the user provided ETL config and generate a pipeline specification from it.
   * It will also register all plugins used by the pipeline and create any error datasets used by the pipeline.
   *
   * A valid pipeline has the following properties:
   *
   * All stages in the pipeline have a unique name.
   * Source stages have at least one output and no inputs.
   * Sink stages have at least one input and no outputs.
   * There are no cycles in the pipeline.
   * All inputs into a stage have the same schema.
   *
   * @param config user provided ETL config
   */
  public abstract P generateSpec(C config);

  /**
   * Performs most of the validation and configuration needed by a pipeline.
   * Handles stages, connections, resources, and stage logging settings.
   *
   * @param config user provided ETL config
   * @param specBuilder builder for creating a pipeline spec.
   */
  protected void configureStages(ETLConfig config, PipelineSpec.Builder specBuilder) {
    // validate the config and determine the order we should configure the stages in.
    ValidatedPipeline validatedPipeline = validateConfig(config);
    List<ETLStage> traversalOrder = validatedPipeline.getTraversalOrder();

    Map<String, DefaultPipelineConfigurer<T>> pluginConfigurers = new HashMap<>(traversalOrder.size());
    Map<String, String> pluginTypes = new HashMap<>(traversalOrder.size());
    for (ETLStage stage : traversalOrder) {
      String stageName = stage.getName();
      pluginTypes.put(stageName, stage.getPlugin().getType());
      pluginConfigurers.put(stageName, new DefaultPipelineConfigurer<>(configurer, stageName, engine));
    }

    // anything prefixed by 'system.[engine].' is a pipeline property.
    Map<String, String> pipelineProperties = new HashMap<>();
    String prefix = String.format("system.%s.", engine.name().toLowerCase());
    int prefixLength = prefix.length();
    for (Map.Entry<String, String> property : config.getProperties().entrySet()) {
      if (property.getKey().startsWith(prefix)) {
        String strippedKey = property.getKey().substring(prefixLength);
        pipelineProperties.put(strippedKey, property.getValue());
      }
    }

    // row = property name, column = property value, val = stage that set the property
    // this is used so that we can error with a nice message about which stages are setting conflicting properties
    Table<String, String, String> propertiesFromStages = HashBasedTable.create();
    // configure the stages in order and build up the stage specs
    for (ETLStage stage : traversalOrder) {
      String stageName = stage.getName();
      DefaultPipelineConfigurer<T> pluginConfigurer = pluginConfigurers.get(stageName);

      ConfiguredStage configuredStage = configureStage(stage, validatedPipeline, pluginConfigurer);

      // for each output, set their input schema to our output schema
      for (String nextStageName : validatedPipeline.getOutputs(stageName)) {

        String nextStageType = pluginTypes.get(nextStageName);

        DefaultStageConfigurer outputStageConfigurer = pluginConfigurers.get(nextStageName).getStageConfigurer();

        // if the output stage is an error transform, it takes the error schema of this stage as its input.
        // if the current stage is a splitter transform, it takes the output schema of the port it is connected to
        // all other plugin types that the output schema of this stage as its input.
        Schema nextStageInputSchema;
        if (ErrorTransform.PLUGIN_TYPE.equals(nextStageType)) {
          nextStageInputSchema = configuredStage.stageSpec.getErrorSchema();
        } else if (SplitterTransform.PLUGIN_TYPE.equals(configuredStage.stageSpec.getPlugin().getType())) {
          StageSpec.Port portSpec = configuredStage.stageSpec.getOutputPorts().get(nextStageName);
          // port can be null if no output ports were specified at configure time
          // this can happen if the ports are dependent on the data received by the plugin
          if (portSpec == null) {
            nextStageInputSchema = null;
          } else if (portSpec.getPort() == null) {
            // null if a splitter was connected to another stage without a port specified.
            // Should not happen since it should have been validated earlier, but check here just in case
            throw new IllegalArgumentException(
              String.format("Must specify a port when connecting Splitter '%s' to '%s'", stageName, nextStageName));
          } else {
            nextStageInputSchema = portSpec.getSchema();
          }
        } else {
          nextStageInputSchema = configuredStage.stageSpec.getOutputSchema();
        }

        // Do not allow null input schema for Joiner
//        if (BatchJoiner.PLUGIN_TYPE.equals(nextStageType) && nextStageInputSchema == null) {
//          throw new IllegalArgumentException(String.format("Joiner cannot have any null input schemas, 
//        but stage %s " +
//                                                             "outputs a null schema.", stageName));
//        }

        // Do not allow more than one input schema for stages other than Joiner and Action
        if (!BatchJoiner.PLUGIN_TYPE.equals(nextStageType)
          && !SparkJoiner.PLUGIN_TYPE.equals(nextStageType)
          && !Action.PLUGIN_TYPE.equals(nextStageType)
          && !Condition.PLUGIN_TYPE.equals(nextStageType)
          && !hasSameSchema(outputStageConfigurer.getInputSchemas(), nextStageInputSchema)) {
          throw new IllegalArgumentException("Two different input schema were set for the stage " + nextStageName);
        }

        outputStageConfigurer.addInputSchema(stageName, nextStageInputSchema);
      }
      specBuilder.addStage(configuredStage.stageSpec);
      for (Map.Entry<String, String> propertyEntry : configuredStage.pipelineProperties.entrySet()) {
        propertiesFromStages.put(propertyEntry.getKey(), propertyEntry.getValue(), stageName);
      }
    }

    // check that multiple stages did not set conflicting properties
    for (String propertyName : propertiesFromStages.rowKeySet()) {
      // go through all values set for the property name. If there is more than one, we have a conflict.
      Map<String, String> propertyValues = propertiesFromStages.row(propertyName);
      if (propertyValues.size() > 1) {
        StringBuilder errMsg = new StringBuilder("Pipeline property '")
          .append(propertyName)
          .append("' is being set to different values by stages.");
        for (Map.Entry<String, String> valueEntry : propertyValues.entrySet()) {
          String propertyValue = valueEntry.getKey();
          String fromStage = valueEntry.getValue();
          errMsg.append(" stage '").append(fromStage).append("' = '").append(propertyValue).append("',");
        }
        errMsg.deleteCharAt(errMsg.length() - 1);
        throw new IllegalArgumentException(errMsg.toString());
      }
      pipelineProperties.put(propertyName, propertyValues.keySet().iterator().next());
    }

    specBuilder.addConnections(config.getConnections())
      .setResources(config.getResources())
      .setDriverResources(config.getDriverResources())
      .setClientResources(config.getClientResources())
      .setStageLoggingEnabled(config.isStageLoggingEnabled())
      .setNumOfRecordsPreview(config.getNumOfRecordsPreview())
      .setProperties(pipelineProperties)
      .build();
  }

  private boolean hasSameSchema(Map<String, Schema> inputSchemas, Schema inputSchema) {
    if (!inputSchemas.isEmpty()) {
      if (!Objects.equals(inputSchemas.values().iterator().next(), inputSchema)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Configures a stage and returns the spec for it.
   *
   * @param stage the user provided configuration for the stage
   * @param validatedPipeline the validated pipeline config
   * @param pluginConfigurer configurer used to configure the stage
   * @return the spec for the stage
   */
  private ConfiguredStage configureStage(ETLStage stage, ValidatedPipeline validatedPipeline,
                                         DefaultPipelineConfigurer<T> pluginConfigurer) {
    String stageName = stage.getName();
    ETLPlugin stagePlugin = stage.getPlugin();

    PluginSpec pluginSpec = configurePlugin(stageName, stagePlugin, pluginConfigurer);
    DefaultStageConfigurer stageConfigurer = pluginConfigurer.getStageConfigurer();
    Map<String, StageSpec.Port> outputSchemas = new HashMap<>();
    Map<String, Schema> inputSchemas = stageConfigurer.getInputSchemas();
    if (pluginSpec.getType().equals(SplitterTransform.PLUGIN_TYPE)) {
      Map<String, Schema> outputPortSchemas = stageConfigurer.getOutputPortSchemas();
      for (Map.Entry<String, String> outputEntry : validatedPipeline.getOutputPorts(stageName).entrySet()) {
        String outputStage = outputEntry.getKey();
        String outputPort = outputEntry.getValue();
        if (outputPort == null) {
          throw new IllegalArgumentException(String.format("Connection from Splitter '%s' to '%s' must specify a port.",
                                                           stageName, outputStage));
        }
        outputSchemas.put(outputStage, new StageSpec.Port(outputPort, outputPortSchemas.get(outputPort)));
      }
    } else {
      Schema outputSchema = stageConfigurer.getOutputSchema();
      // conditions handled specially since they can have an action and a transform as input, where they might have
      // different input schemas. picking the first non-null schema, or null if they're all null
      // this isn't perfect, as we should really be checking all non-action input schemas and validating that they're
      // all the same
      if (Condition.PLUGIN_TYPE.equals(pluginSpec.getType())) {
        outputSchema = null;
        for (Schema schema : inputSchemas.values()) {
          if (schema != null) {
            // this check isn't perfect, as we should still error if 2 transforms are inputs,
            // one has null schema and another has non-null schema.
            // todo: fix this cleanly and fully
            if (outputSchema != null && !outputSchema.equals(schema)) {
              throw new IllegalArgumentException("Cannot have different input schemas going into stage " + stageName);
            }
            outputSchema = schema;
          }
        }
      }

      for (String outputStage : validatedPipeline.getOutputs(stageName)) {
        outputSchemas.put(outputStage, new StageSpec.Port(null, outputSchema));
      }
    }

    StageSpec stageSpec = StageSpec.builder(stageName, pluginSpec)
      .addInputSchemas(inputSchemas)
      .addOutputPortSchemas(outputSchemas)
      .setErrorSchema(stageConfigurer.getErrorSchema())
      .setProcessTimingEnabled(validatedPipeline.isProcessTimingEnabled())
      .setStageLoggingEnabled(validatedPipeline.isStageLoggingEnabled())
      .build();
    return new ConfiguredStage(stageSpec, pluginConfigurer.getPipelineProperties());
  }


  /**
   * Configures a plugin and returns the spec for it.
   *
   * @param pluginId the unique plugin id
   * @param etlPlugin user provided configuration for the plugin
   * @param pipelineConfigurer default pipeline configurere to configure the plugin
   * @return the spec for the plugin
   */
  protected PluginSpec configurePlugin(String pluginId, ETLPlugin etlPlugin,
                                       DefaultPipelineConfigurer<T> pipelineConfigurer) {
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider(etlPlugin.getType(), etlPlugin.getName())
        .getPluginSelector(etlPlugin.getArtifactConfig()));
    String type = etlPlugin.getType();
    Object plugin = configurer.usePlugin(etlPlugin.getType(),
                                         etlPlugin.getName(),
                                         pluginId,
                                         etlPlugin.getPluginProperties(),
                                         pluginSelector);

    if (plugin == null) {
      throw new IllegalArgumentException(
        String.format("No plugin of type %s and name %s could be found for stage %s.",
                      etlPlugin.getType(), etlPlugin.getName(), pluginId));
    }
    try {
      if (type.equals(BatchJoiner.PLUGIN_TYPE) || type.equals(SparkJoiner.PLUGIN_TYPE)) {
        MultiInputPipelineConfigurable multiPlugin = (MultiInputPipelineConfigurable) plugin;
        multiPlugin.configurePipeline(pipelineConfigurer);
      } else if (type.equals(SplitterTransform.PLUGIN_TYPE)) {
        MultiOutputPipelineConfigurable multiOutputPlugin = (MultiOutputPipelineConfigurable) plugin;
        multiOutputPlugin.configurePipeline(pipelineConfigurer);
      } else if (!type.equals(Constants.SPARK_PROGRAM_PLUGIN_TYPE)) {
        PipelineConfigurable singlePlugin = (PipelineConfigurable) plugin;
        singlePlugin.configurePipeline(pipelineConfigurer);
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error configuring stage '%s': %s", pluginId, e.getMessage()), e);
    }
    return new PluginSpec(etlPlugin.getType(),
                          etlPlugin.getName(),
                          etlPlugin.getProperties(),
                          pluginSelector.getSelectedArtifact());
  }

  /**
   * Validate that this is a valid pipeline. A valid pipeline has the following properties:
   *
   * All stages in the pipeline have a unique name.
   * Source stages have at least one output and no inputs.
   * Sink stages have at least one input and no outputs.
   * There are no cycles in the pipeline.
   * All inputs into a stage have the same schema.
   * ErrorTransforms only have BatchSource, Transform, or BatchAggregator as input stages.
   * AlertPublishers have at least one input and no outputs and don't have SparkSink or BatchSink as input.
   * Action stages can only be at the start or end of the pipeline.
   * Condition stages have at most 2 outputs. Each stage on a condition's output branch has at most a single input.
   *
   * Returns the stages in the order they should be configured to ensure that all input stages are configured
   * before their output.
   *
   * @param config the user provided configuration
   * @return the order to configure the stages in
   * @throws IllegalArgumentException if the pipeline is invalid
   */
  private ValidatedPipeline validateConfig(ETLConfig config) {
    config.validate();
    if (config.getStages().isEmpty()) {
      throw new IllegalArgumentException("A pipeline must contain at least one stage.");
    }

    Set<String> actionStages = new HashSet<>();
    Set<String> conditionStages = new HashSet<>();
    Map<String, String> stageTypes = new HashMap<>();
    // check stage name uniqueness
    Set<String> stageNames = new HashSet<>();
    for (ETLStage stage : config.getStages()) {
      if (!stageNames.add(stage.getName())) {
        throw new IllegalArgumentException(
          String.format("Invalid pipeline. Multiple stages are named %s. Please ensure all stage names are unique",
                        stage.getName()));
      }
      // if stage is Action stage, add it to the Action stage set
      if (isAction(stage.getPlugin().getType())) {
        actionStages.add(stage.getName());
      }
      // if the stage is condition add it to the Condition stage set
      if (stage.getPlugin().getType().equals(Condition.PLUGIN_TYPE)) {
        conditionStages.add(stage.getName());
      }

      stageTypes.put(stage.getName(), stage.getPlugin().getType());
    }

    // check that the from and to are names of actual stages
    // also check that conditions have at most 2 outgoing connections each label with true or
    // false but not both
    Map<String, Boolean> conditionBranch = new HashMap<>();
    for (Connection connection : config.getConnections()) {
      if (!stageNames.contains(connection.getFrom())) {
        throw new IllegalArgumentException(
          String.format("Invalid connection %s. %s is not a stage.", connection, connection.getFrom()));
      }
      if (!stageNames.contains(connection.getTo())) {
        throw new IllegalArgumentException(
          String.format("Invalid connection %s. %s is not a stage.", connection, connection.getTo()));
      }

      if (conditionStages.contains(connection.getFrom())) {
        if (connection.getCondition() == null) {
          String msg = String.format("For condition stage %s, the connection %s is not marked with either " +
                                       "'true' or 'false'.", connection.getFrom(), connection);
          throw new IllegalArgumentException(msg);
        }

        // check if connection from the condition node is marked as true or false multiple times
        if (conditionBranch.containsKey(connection.getFrom())
          && connection.getCondition().equals(conditionBranch.get(connection.getFrom()))) {
          String msg = String.format("For condition stage '%s', more than one outgoing connections are marked as %s.",
                                     connection.getFrom(), connection.getCondition());
          throw new IllegalArgumentException(msg);
        }
        conditionBranch.put(connection.getFrom(), connection.getCondition());
      }
    }

    List<ETLStage> traversalOrder = new ArrayList<>(stageNames.size());

    // can only have empty connections if the pipeline consists of a single action.
    if (config.getConnections().isEmpty()) {
      if (actionStages.size() == 1 && stageNames.size() == 1) {
        traversalOrder.add(config.getStages().iterator().next());
        return new ValidatedPipeline(traversalOrder, config);
      } else {
        throw new IllegalArgumentException(
          "Invalid pipeline. There are no connections between stages. " +
            "This is only allowed if the pipeline consists of a single action plugin.");
      }
    }

    Dag dag = new Dag(config.getConnections());

    Set<String> controlStages = Sets.union(actionStages, conditionStages);
    Map<String, ETLStage> stages = new HashMap<>();
    for (ETLStage stage : config.getStages()) {
      String stageName = stage.getName();
      Set<String> stageInputs = dag.getNodeInputs(stageName);
      Set<String> stageOutputs = dag.getNodeOutputs(stageName);
      String stageType = stage.getPlugin().getType();

      boolean isSource = isSource(stageType);
      boolean isSink = isSink(stageType);
      // check source plugins are sources in the dag
      if (isSource) {
        if (!stageInputs.isEmpty() && !controlStages.containsAll(stageInputs)) {
          throw new IllegalArgumentException(
            String.format("%s %s has incoming connections from %s. %s stages cannot have any incoming connections.",
                          stageType, stageName, stageType, Joiner.on(',').join(stageInputs)));
        }
      } else if (isSink) {
        if (!stageOutputs.isEmpty() && !controlStages.containsAll(stageOutputs)) {
          throw new IllegalArgumentException(
            String.format("%s %s has outgoing connections to %s. %s stages cannot have any outgoing connections.",
                          stageType, stageName, stageType, Joiner.on(',').join(stageOutputs)));
        }
      } else if (ErrorTransform.PLUGIN_TYPE.equals(stageType)) {
        for (String inputStage : stageInputs) {
          String inputType = stageTypes.get(inputStage);
          if (!VALID_ERROR_INPUTS.contains(inputType)) {
            throw new IllegalArgumentException(String.format(
              "ErrorTransform %s cannot have stage %s of type %s as input. Only %s stages can emit errors.",
              stageName, inputStage, inputType, Joiner.on(',').join(VALID_ERROR_INPUTS)));
          }
        }
      }

      boolean isAction = isAction(stageType);
      if (!isAction && !stageType.equals(Condition.PLUGIN_TYPE) && !isSource && stageInputs.isEmpty()) {
        throw new IllegalArgumentException(
          String.format("Stage %s is unreachable, it has no incoming connections.", stageName));
      }
      if (!isAction && !isSink && stageOutputs.isEmpty()) {
        throw new IllegalArgumentException(
          String.format("Stage %s is a dead end, it has no outgoing connections.", stageName));
      }

      stages.put(stageName, stage);
    }

    validateConditionBranches(conditionStages, dag);

    for (String stageName : dag.getTopologicalOrder()) {
      traversalOrder.add(stages.get(stageName));
    }

    return new ValidatedPipeline(traversalOrder, config);
  }

  private boolean isAction(String pluginType) {
    return Action.PLUGIN_TYPE.equals(pluginType) || Constants.SPARK_PROGRAM_PLUGIN_TYPE.equals(pluginType);
  }

  private boolean isSource(String pluginType) {
    return sourcePluginTypes.contains(pluginType);
  }

  private boolean isSink(String pluginType) {
    return sinkPluginTypes.contains(pluginType);
  }

  /**
   * Just a container for StageSpec and pipeline properties set by the stage
   */
  private static class ConfiguredStage {
    private final StageSpec stageSpec;
    private final Map<String, String> pipelineProperties;

    private ConfiguredStage(StageSpec stageSpec, Map<String, String> pipelineProperties) {
      this.stageSpec = stageSpec;
      this.pipelineProperties = pipelineProperties;
    }
  }

  /**
   * Make sure that the stages on the condition branches do not have more than one incoming connections
   * @param conditionStages the set of condition stages in the pipeline
   * @param dag the dag of connections
   * @throws IllegalArgumentException if there is an error in the configurations, for example condition stage
   * is not configured correctly to have 'true' and 'false' branches or there are multiple incoming connections
   * on the stages which are on condition branches
   */
  private void validateConditionBranches(Set<String> conditionStages, Dag dag) {
    for (String conditionStage : conditionStages) {
      // Get the output connection stages for this condition. Should be max 2
      Set<String> outputs = dag.getNodeOutputs(conditionStage);
      if (outputs == null || outputs.size() > 2) {
        String msg = String.format("Condition stage in the pipeline '%s' should have at least 1 and at max 2 " +
                                     "outgoing connections corresponding to 'true' and 'false' branches " +
                                     "but found '%s'.", conditionStage, outputs == null ? 0 : outputs.size());
        throw new IllegalArgumentException(msg);
      }
      for (String output : outputs) {
        // Check that each stage in this branch starting from output has only one incoming connection
        validateSingleInput(conditionStage, output, dag);
      }
    }
  }

  /**
   * Current stage can only have either one incoming connection if its one of the stage on the condition branch or
   * two incoming connections if it is condition stopper stage. We do not allow cross connections from sources.
   */
  private void validateSingleInput(String currentCondition, String currentStage, Dag dag) {
    if (dag.getNodeInputs(currentStage).size() > 1) {
      Set<String> stopNodes = new HashSet<>(dag.getSources());
      stopNodes.add(currentCondition);
      Set<String> parents = dag.parentsOf(currentStage, stopNodes);
      parents.retainAll(dag.getSources());
      if (parents.size() > 0) {
        String paths = "";
        for (String parent : parents) {
          if (!paths.isEmpty()) {
            paths = paths + ", ";
          }
          paths += parent + "->" + currentStage;
        }
        String msg = String.format("Stage in the pipeline '%s' is on the branch of condition '%s'. However it also " +
                                     "has following incoming paths: '%s', which is not supported.", currentStage,
                                   currentCondition, paths);
        throw new IllegalArgumentException(msg);
      }
    }

    Set<String> outputStages = dag.getNodeOutputs(currentStage);
    if (outputStages.size() == 0) {
      // We reached sink. simply return
      return;
    }
    for (String output : outputStages) {
      validateSingleInput(currentCondition, output, dag);
    }
  }
}
