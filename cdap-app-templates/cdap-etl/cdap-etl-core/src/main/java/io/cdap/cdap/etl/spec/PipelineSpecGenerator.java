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

package io.cdap.cdap.etl.spec;

import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.FeatureFlagsProvider;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurable;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.condition.Condition;
import io.cdap.cdap.etl.api.join.AutoJoiner;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.common.ConnectionRegistryMacroEvaluator;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAutoJoinerContext;
import io.cdap.cdap.etl.common.DefaultPipelineConfigurer;
import io.cdap.cdap.etl.common.DefaultStageConfigurer;
import io.cdap.cdap.etl.common.Schemas;
import io.cdap.cdap.etl.planner.Dag;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionBadRequestException;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is run at application configure time to take an application config {@link ETLConfig} and call
 * {@link PipelineConfigurable#configurePipeline(PipelineConfigurer)} on all plugins in the pipeline.
 * This generates a {@link PipelineSpec} which the programs understand.
 *
 * @param <C> the type of user provided config
 * @param <P> the pipeline specification generated from the config
 */
public abstract class PipelineSpecGenerator<C extends ETLConfig, P extends PipelineSpec> {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineSpecGenerator.class);
  private static final Set<String> VALID_ERROR_INPUTS = ImmutableSet.of(
    BatchSource.PLUGIN_TYPE, Transform.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE, ErrorTransform.PLUGIN_TYPE);
  protected final PluginConfigurer pluginConfigurer;
  protected final DatasetConfigurer datasetConfigurer;
  protected final FeatureFlagsProvider featureFlagProvider;
  protected final Engine engine;
  private final Set<String> sourcePluginTypes;
  private final Set<String> sinkPluginTypes;
  private final ConnectionRegistryMacroEvaluator connectionEvaluator;
  private final MacroParserOptions options;


  protected <T extends PluginConfigurer & DatasetConfigurer & FeatureFlagsProvider>
  PipelineSpecGenerator(T configurer, Set<String> sourcePluginTypes, Set<String> sinkPluginTypes, Engine engine) {
    this.pluginConfigurer = configurer;
    this.datasetConfigurer = configurer;
    this.featureFlagProvider = configurer;
    this.sourcePluginTypes = sourcePluginTypes;
    this.sinkPluginTypes = sinkPluginTypes;
    this.engine = engine;
    this.connectionEvaluator = new ConnectionRegistryMacroEvaluator();
    this.options = MacroParserOptions.builder().skipInvalidMacros().setEscaping(false)
                     .setFunctionWhitelist(ConnectionRegistryMacroEvaluator.FUNCTION_NAME).build();
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
   * @throws ValidationException if the pipeline is invalid
   */
  public abstract P generateSpec(C config) throws ValidationException;

  /**
   * Performs most of the validation and configuration needed by a pipeline.
   * Handles stages, connections, resources, and stage logging settings.
   *
   * @param config user provided ETL config
   * @param specBuilder builder for creating a pipeline spec.
   * @throws ValidationException if the pipeline is invalid
   */
  protected void configureStages(ETLConfig config, PipelineSpec.Builder specBuilder) throws ValidationException {
    // validate the config and determine the order we should configure the stages in.
    ValidatedPipeline validatedPipeline = validateConfig(config);
    List<ETLStage> traversalOrder = validatedPipeline.getTraversalOrder();

    Map<String, DefaultPipelineConfigurer> pluginConfigurers = new HashMap<>(traversalOrder.size());
    Map<String, String> pluginTypes = new HashMap<>(traversalOrder.size());
    for (ETLStage stage : traversalOrder) {
      String stageName = stage.getName();
      pluginTypes.put(stageName, stage.getPlugin().getType());
      pluginConfigurers.put(stageName, new DefaultPipelineConfigurer(pluginConfigurer, datasetConfigurer,
                                                                     stageName, engine,
                                                                     new DefaultStageConfigurer(stageName),
                                                                     featureFlagProvider));
    }
    SchemaPropagator schemaPropagator = new SchemaPropagator(pluginConfigurers, validatedPipeline::getOutputs,
                                                             pluginTypes::get);

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
      DefaultPipelineConfigurer pluginConfigurer = pluginConfigurers.get(stageName);

      ConfiguredStage configuredStage = configureStage(stage, validatedPipeline, pluginConfigurer);
      schemaPropagator.propagateSchema(configuredStage.getStageSpec());

      specBuilder.addStage(configuredStage.getStageSpec());
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
      .addConnectionsUsed(connectionEvaluator.getConnectionIds())
      .build();
  }

  /**
   * Configures a stage and returns the spec for it.
   *
   * @param stage the user provided configuration for the stage
   * @param validatedPipeline the validated pipeline config
   * @param pluginConfigurer configurer used to configure the stage
   * @return the spec for the stage
   * @throws ValidationException if the plugin threw an exception during configuration
   */
  protected ConfiguredStage configureStage(ETLStage stage, ValidatedPipeline validatedPipeline,
                                           DefaultPipelineConfigurer pluginConfigurer) throws ValidationException {
    String stageName = stage.getName();
    ETLPlugin stagePlugin = stage.getPlugin();

    StageSpec.Builder specBuilder = configureStage(stageName, stagePlugin, pluginConfigurer);
    DefaultStageConfigurer stageConfigurer = pluginConfigurer.getStageConfigurer();
    String pluginType = stage.getPlugin().getType();
    if (pluginType.equals(SplitterTransform.PLUGIN_TYPE)) {
      Map<String, Schema> outputPortSchemas = stageConfigurer.getOutputPortSchemas();
      for (Map.Entry<String, String> outputEntry : validatedPipeline.getOutputPorts(stageName).entrySet()) {
        String outputStage = outputEntry.getKey();
        String outputPort = outputEntry.getValue();
        if (outputPort == null) {
          throw new IllegalArgumentException(String.format("Connection from Splitter '%s' to '%s' must specify a port.",
                                                           stageName, outputStage));
        }
        specBuilder.addOutput(outputStage, outputPort, outputPortSchemas.get(outputPort));
      }
    } else {
      Schema outputSchema = stageConfigurer.getOutputSchema();
      // conditions handled specially since they can have an action and a transform as input, where they might have
      // different input schemas. picking the first non-null schema, or null if they're all null
      // this isn't perfect, as we should really be checking all non-action input schemas and validating that they're
      // all the same
      if (Condition.PLUGIN_TYPE.equals(pluginType)) {
        outputSchema = null;
        for (Schema schema : stageConfigurer.getInputSchemas().values()) {
          if (schema != null) {
            // this check isn't perfect, as we should still error if 2 transforms are inputs,
            // one has null schema and another has non-null schema.
            // todo: fix this cleanly and fully
            if (outputSchema != null && !Schemas.equalsIgnoringRecordName(outputSchema, schema)) {
              throw new IllegalArgumentException("Cannot have different input schemas going into stage " + stageName);
            }
            outputSchema = schema;
          }
        }
      }

      for (String outputStage : validatedPipeline.getOutputs(stageName)) {
        specBuilder.addOutput(outputStage, null, outputSchema);
      }
    }

    StageSpec stageSpec = specBuilder
      .setProcessTimingEnabled(validatedPipeline.isProcessTimingEnabled())
      .setStageLoggingEnabled(validatedPipeline.isStageLoggingEnabled())
      .setMaxPreviewRecords(validatedPipeline.getMaxPreviewRecords())
      .build();
    return new ConfiguredStage(stageSpec, pluginConfigurer.getPipelineProperties());
  }


  /**
   * Configures a plugin and returns the spec for it.
   *
   * @param stageName the unique plugin id
   * @param etlPlugin user provided configuration for the plugin
   * @param pipelineConfigurer default pipeline configurer to configure the plugin
   * @return the spec for the plugin
   * @throws IllegalArgumentException if the plugin with same id is already deployed
   * @throws ValidationException if the plugin threw an exception during configuration
   */
  public StageSpec.Builder configureStage(String stageName, ETLPlugin etlPlugin,
                                          DefaultPipelineConfigurer pipelineConfigurer) throws ValidationException {
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(etlPlugin.getArtifactConfig()));
    String type = etlPlugin.getType();
    String pluginName = etlPlugin.getName();

    DefaultStageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    Object plugin = getPlugin(stageName, etlPlugin, pluginSelector, type, pluginName, collector);
    try {
      if (type.equals(BatchJoiner.PLUGIN_TYPE)) {
        MultiInputPipelineConfigurable multiPlugin = (MultiInputPipelineConfigurable) plugin;
        multiPlugin.configurePipeline(pipelineConfigurer);
        // A 'batchJoiner' plugin can be either a BatchJoiner or a BatchAutoJoiner.
        // This is because we want to allow a Joiner plugin to switch from using the BatchJoiner interface
        // to the BatchAutoJoiner while preserving backwards compatibility in the pipeline config.
        if (plugin instanceof AutoJoiner) {
          configureAutoJoiner(stageName, (AutoJoiner) plugin, stageConfigurer, collector);
        }
      } else if (type.equals(SplitterTransform.PLUGIN_TYPE)) {
        MultiOutputPipelineConfigurable multiOutputPlugin = (MultiOutputPipelineConfigurable) plugin;
        multiOutputPlugin.configurePipeline(pipelineConfigurer);
      } else if (!type.equals(Constants.SPARK_PROGRAM_PLUGIN_TYPE)) {
        PipelineConfigurable singlePlugin = (PipelineConfigurable) plugin;
        singlePlugin.configurePipeline(pipelineConfigurer);
        // we don't have StreamingSource dependency so use source plugin types to check type
        // evaluate macros and find out if there is connection used
        if (sourcePluginTypes.contains(type) || BatchSink.PLUGIN_TYPE.equals(type)) {
          pluginConfigurer.evaluateMacros(etlPlugin.getProperties(), connectionEvaluator, options);
        }
      }
    } catch (InvalidConfigPropertyException e) {
      collector.addFailure(e.getMessage(),
                           String.format("Provide valid value for config property '%s'.", e.getProperty()))
        .withConfigProperty(e.getProperty());
    } catch (InvalidStageException e) {
      if (e.getReasons().isEmpty()) {
        collector.addFailure(e.getMessage(), null);
      }

      for (InvalidStageException reason : e.getReasons()) {
        if (reason instanceof InvalidConfigPropertyException) {
          InvalidConfigPropertyException configException = (InvalidConfigPropertyException) reason;
          collector.addFailure(configException.getMessage(),
                               String.format("Provide valid value for config property '%s'.",
                                             configException.getProperty()))
            .withConfigProperty(configException.getProperty());
        } else {
          collector.addFailure(reason.getMessage(), null);
        }
      }
    } catch (ValidationException e) {
      throw e;
    } catch (NullPointerException e) {
      // handle the case where plugin throws null pointer exception, this is to avoid having 'null' as error message
      collector.addFailure(String.format("Null error occurred while configuring the stage %s.", stageName), null)
        .withStacktrace(e.getStackTrace());
    } catch (ArrayIndexOutOfBoundsException e) {
      // handle the case where plugin throws index out of bounds exception,
      // this is to avoid having a number like '2', '8' etc as error message
      collector.addFailure(String.format("Index out of bounds error occurred while configuring the stage %s.",
                                         stageName), null).withStacktrace(e.getStackTrace());
    } catch (ConnectionBadRequestException e) {
      collector.addFailure(e.getMessage(), "Provide a valid connection name.");
    } catch (Exception e) {
      collector.addFailure(String.format("Error encountered while configuring the stage: '%s'",
                                         e.getMessage()), null).withStacktrace(e.getStackTrace());
    }

    // throw validation exception if there are any errors being carried by failure collector
    collector.getOrThrowException();

    PluginSpec pluginSpec = new PluginSpec(type, pluginName, etlPlugin.getProperties(),
                                           pluginSelector.getSelectedArtifact());
    StageSpec.Builder specBuilder = StageSpec.builder(stageName, pluginSpec)
      .addInputSchemas(pipelineConfigurer.getStageConfigurer().getInputSchemas())
      .setErrorSchema(stageConfigurer.getErrorSchema());

    if (type.equals(SplitterTransform.PLUGIN_TYPE)) {
      specBuilder.setPortSchemas(stageConfigurer.getOutputPortSchemas());
    } else {
      specBuilder.setOutputSchema(stageConfigurer.getOutputSchema());
    }
    return specBuilder;
  }

  protected void validateJoinCondition(String stageName, JoinCondition condition, FailureCollector collector) {
    if (engine == Engine.MAPREDUCE && condition.getOp() != JoinCondition.Op.KEY_EQUALITY) {
      collector.addFailure(
        String.format("Join stage '%s' uses a %s condition, which is not supported with the MapReduce engine.",
                      stageName, condition.getOp()),
        "Switch to a different execution engine.");
    }
  }

  private void configureAutoJoiner(String stageName, AutoJoiner autoJoiner, DefaultStageConfigurer stageConfigurer,
                                   FailureCollector collector) {
    AutoJoinerContext autoContext = DefaultAutoJoinerContext.from(stageConfigurer.getInputSchemas(),
                                                                  collector);
    JoinDefinition joinDefinition = autoJoiner.define(autoContext);
    if (joinDefinition == null) {
      return;
    }

    validateJoinCondition(stageName, joinDefinition.getCondition(), collector);

    stageConfigurer.setOutputSchema(joinDefinition.getOutputSchema());
    Set<String> inputStages = stageConfigurer.getInputSchemas().keySet();
    Set<String> joinStages = joinDefinition.getStages().stream()
      .map(JoinStage::getStageName)
      .collect(Collectors.toSet());
    Set<String> missingInputs = Sets.difference(inputStages, joinStages);
    if (!missingInputs.isEmpty()) {
      collector.addFailure(String.format("Joiner stage '%s' did not include input stage %s in the join.",
                                         stageName, String.join(", ", missingInputs)),
                           "Check with the plugin developer to make sure it is implemented correctly.");
    }
    Set<String> extraInputs = Sets.difference(joinStages, inputStages);
    if (!extraInputs.isEmpty()) {
      collector.addFailure(String.format("Joiner stage '%s' is trying to join stage %s, which is not an input.",
                                         stageName, String.join(", ", missingInputs)),
                           "Check with the plugin developer to make sure it is implemented correctly.");
    }
  }

  /**
   * Adds a Plugin usage to the Application and create a new instance.
   *
   * @param stageName stage name
   * @param etlPlugin plugin
   * @param pluginSelector plugin selector
   * @param type type of the plugin
   * @param pluginName plugin name
   * @param collector failure collector
   * @throws ValidationException if error while creating new plugin instance
   * @return new instance of the plugin
   */
  private Object getPlugin(String stageName, ETLPlugin etlPlugin, TrackedPluginSelector pluginSelector, String type,
                           String pluginName, FailureCollector collector) {
    Object plugin = null;
    try {
      // Call to usePlugin may throw IllegalArgumentException if hte plugin with the same id is already deployed.
      // This would mean there is a bug in the app and this can not be fixed by user. That is why it is not handled as
      // a ValidationFailure.
      plugin = pluginConfigurer.usePlugin(type, pluginName, stageName, etlPlugin.getPluginProperties(), pluginSelector);
    } catch (InvalidPluginConfigException e) {
      int numFailures = 0;
      for (String missingProperty : e.getMissingProperties()) {
        collector.addFailure(String.format("Required property '%s' has no value.", missingProperty), null)
          .withConfigProperty(missingProperty);
        numFailures++;
      }
      for (InvalidPluginProperty invalidProperty : e.getInvalidProperties()) {
        collector.addFailure(e.getMessage(), null)
          .withConfigProperty(invalidProperty.getName());
        numFailures++;
      }
      // if plugin instantiation didn't fail because of a missing property or an invalid property,
      // create a generic failure
      if (numFailures == 0) {
        collector.addFailure(e.getMessage(), null);
      }
    } catch (Exception e) {
      // TODO: Catch specific exceptions when CDAP-15744 is fixed
      collector.addFailure(e.getMessage(), null).withStacktrace(e.getStackTrace());
    }

    // throw validation exception if any error occurred while creating a new instance of the plugin
    collector.getOrThrowException();

    if (plugin == null) {
      String errorMessage = String.format("Plugin named '%s' of type '%s' not found.", pluginName, type);
      String correctiveAction = String.format("Make sure plugin '%s' of type '%s' is already deployed.",
                                              pluginName, type);
      ArtifactSelectorConfig requested = etlPlugin.getArtifactConfig();
      ArtifactId requestedArtifactId = requested == null ? null :
        new ArtifactId(requested.getName(), new ArtifactVersion(requested.getVersion()),
                       ArtifactScope.valueOf(requested.getScope()));

      ArtifactSelectorConfig suggestion = pluginSelector.getSuggestion();
      ArtifactId suggestedArtifactId = null;
      if (suggestion != null) {
        suggestedArtifactId = new ArtifactId(suggestion.getName(), new ArtifactVersion(suggestion.getVersion()),
                                             ArtifactScope.valueOf(suggestion.getScope()));

      }

      collector.addFailure(errorMessage, correctiveAction)
        .withPluginNotFound(stageName, pluginName, type, requestedArtifactId, suggestedArtifactId);

      // throw validation exception if the plugin is not initialized
      collector.getOrThrowException();
    }
    return plugin;
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
  protected ValidatedPipeline validateConfig(ETLConfig config) {
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
                          stageType, stageName, Joiner.on(',').join(stageInputs), stageType));
        }
        // check that source plugins are not present after any non-condition/action stage
        Set<String> parents = dag.parentsOf(stageName);
        Set<String> nonControlParents = Sets.difference(parents, controlStages);
        if (nonControlParents.size() > 1) { // the stage's nonControlParents should only contain itself
          throw new IllegalArgumentException(
            String.format("%s %s is invalid. %s stages can only be placed at the start of the pipeline.",
                          stageType, stageName, stageType));
        }
      } else if (isSink) {
        if (!stageOutputs.isEmpty() && !controlStages.containsAll(stageOutputs)) {
          throw new IllegalArgumentException(
            String.format("%s %s has outgoing connections to %s. %s stages cannot have any outgoing connections.",
                          stageType, stageName, Joiner.on(',').join(stageOutputs), stageType));
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

    // make sure actions are not in the middle of the pipeline -- only at the start and/or end
    for (String actionStage : actionStages) {
      Set<String> actionParents = dag.parentsOf(actionStage);
      Set<String> actionChildren = dag.accessibleFrom(actionStage);
      Set<String> nonControlParents = Sets.difference(actionParents, controlStages);
      Set<String> nonControlChildren = Sets.difference(actionChildren, controlStages);
      if (!nonControlChildren.isEmpty() && !nonControlParents.isEmpty()) {
        throw new IllegalArgumentException(
          String.format("Action stage '%s' is invalid. Actions can only be placed at the start or end of the pipeline.",
                        actionStage));
      }
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
  protected static class ConfiguredStage {
    private final StageSpec stageSpec;
    private final Map<String, String> pipelineProperties;

    private ConfiguredStage(StageSpec stageSpec, Map<String, String> pipelineProperties) {
      this.stageSpec = stageSpec;
      this.pipelineProperties = pipelineProperties;
    }

    public StageSpec getStageSpec() {
      return stageSpec;
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
      parents.remove(currentCondition);
      if (parents.size() > 0) {
        String paths = "";
        for (String parent : parents) {
          if (!paths.isEmpty()) {
            paths = paths + ", ";
          }
          paths += parent + "->" + currentStage;
        }
        String msg = String.format("Stage in the pipeline '%s' is on the branch of condition '%s'. However it also " +
                                     "has following incoming paths: '%s'. Different branches of a condition cannot " +
                                     "be inputs to the same stage.", currentStage, currentCondition, paths);
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
