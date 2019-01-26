/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.DefaultAggregatorContext;
import co.cask.cdap.etl.batch.DefaultJoinerContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.connector.SingleConnectorFactory;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.etl.common.DefaultStageConfigurer;
import co.cask.cdap.etl.common.FieldOperationTypeAdapter;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.submit.AggregatorContextProvider;
import co.cask.cdap.etl.common.submit.CompositeFinisher;
import co.cask.cdap.etl.common.submit.ContextProvider;
import co.cask.cdap.etl.common.submit.Finisher;
import co.cask.cdap.etl.common.submit.JoinerContextProvider;
import co.cask.cdap.etl.common.submit.SubmitterPlugin;
import co.cask.cdap.etl.proto.v2.spec.StageSpec;
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import co.cask.cdap.etl.spec.RuntimePluginDatasetConfigurer;
import co.cask.cdap.etl.spec.SchemaPropagator;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Configures and sets up runs of {@link BatchSparkPipelineDriver}.
 */
public class ETLSpark extends AbstractSpark {
  private static final Logger LOG = LoggerFactory.getLogger(ETLSpark.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
    .registerTypeAdapter(OutputFormatProvider.class, new OutputFormatProviderTypeAdapter())
    .registerTypeAdapter(InputFormatProvider.class, new InputFormatProviderTypeAdapter())
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter())
    .create();

  private final BatchPhaseSpec phaseSpec;
  private Finisher finisher;
  private List<File> cleanupFiles;

  public ETLSpark(BatchPhaseSpec phaseSpec) {
    this.phaseSpec = phaseSpec;
  }

  @Override
  protected void configure() {
    setName(phaseSpec.getPhaseName());
    setDescription(phaseSpec.getDescription());

    // register the plugins at program level so that the program can be failed by the platform early in case of
    // plugin requirements not being meet
    phaseSpec.getPhase().registerPlugins(getConfigurer());

    setMainClass(BatchSparkPipelineDriver.class);

    setExecutorResources(phaseSpec.getResources());
    setDriverResources(phaseSpec.getDriverResources());
    setClientResources(phaseSpec.getClientResources());

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec, BatchPhaseSpec.class));
    setProperties(properties);
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void initialize() throws Exception {
    SparkClientContext context = getContext();
    cleanupFiles = new ArrayList<>();
    List<Finisher> finishers = new ArrayList<>();

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.speculation", "false");
    context.setSparkConf(sparkConf);

    Map<String, String> properties = context.getSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);

    for (Map.Entry<String, String> pipelineProperty : phaseSpec.getPipelineProperties().entrySet()) {
      sparkConf.set(pipelineProperty.getKey(), pipelineProperty.getValue());
    }

    PipelineRuntime pipelineRuntime = new PipelineRuntime(context);
    MacroEvaluator evaluator = new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                                                         context.getLogicalStartTime(), context,
                                                         context.getNamespace());
    SparkBatchSourceFactory sourceFactory = new SparkBatchSourceFactory();
    SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    Map<String, Integer> stagePartitions = new HashMap<>();
    PluginContext pluginContext = new SparkPipelinePluginContext(context, context.getMetrics(),
                                                                 phaseSpec.isStageLoggingEnabled(),
                                                                 phaseSpec.isProcessTimingEnabled());
    PipelinePluginInstantiator pluginInstantiator =
      new PipelinePluginInstantiator(pluginContext, context.getMetrics(), phaseSpec, new SingleConnectorFactory());
    Admin admin = context.getAdmin();


    PipelinePhase phase = phaseSpec.getPhase();
    // for configuring pipeline stages
    RuntimePluginDatasetConfigurer pluginDatasetConfigurer =
      new RuntimePluginDatasetConfigurer(context.getAdmin(), context, evaluator);
    Map<String, DefaultPipelineConfigurer> pluginConfigurers = new HashMap<>(phase.getDag().getNodes().size());
    for (String stageName : phase.getDag().getNodes()) {
      pluginConfigurers.put(stageName, new DefaultPipelineConfigurer(pluginDatasetConfigurer, pluginDatasetConfigurer,
                                                                     stageName, Engine.SPARK));
    }
    SchemaPropagator schemaPropagator = new SchemaPropagator(pluginConfigurers, phase::getStageOutputs,
                                                             stageName -> phase.getStage(stageName).getPluginType(),
                                                             context.getWorkflowToken());

    // Collect field operations emitted by various stages in this MapReduce program
    Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
    Set<StageSpec> updatedStageSpecs = new HashSet<>(pluginConfigurers.size());
    // go through in topological order so that arguments set by one stage are seen by stages after it
    for (String stageName : phase.getDag().getTopologicalOrder()) {
      StageSpec stageSpec = phase.getStage(stageName);
      String pluginType = stageSpec.getPluginType();
      boolean isConnectorSource =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSources().contains(stageName);
      boolean isConnectorSink =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSinks().contains(stageName);
      DefaultPipelineConfigurer pipelineConfigurer = pluginConfigurers.get(stageName);

      SubmitterPlugin submitterPlugin = null;
      if (BatchSource.PLUGIN_TYPE.equals(pluginType) || isConnectorSource) {

        BatchConfigurable<BatchSourceContext> batchSource = pluginInstantiator.newPluginInstance(stageName, evaluator);
        StageSpec updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator,
                                               batchSource::configurePipeline);
        updatedStageSpecs.add(updatedSpec);
        ContextProvider<SparkBatchSourceContext> contextProvider =
          dsContext -> new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, dsContext, updatedSpec);
        submitterPlugin = new SubmitterPlugin<>(stageName, context, batchSource, contextProvider,
                                                ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));

      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

        Transform transform = pluginInstantiator.newPluginInstance(stageName, evaluator);
        StageSpec updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator,
                                               transform::configurePipeline);
        updatedStageSpecs.add(updatedSpec);
        ContextProvider<SparkBatchSourceContext> contextProvider =
          dsContext -> new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, dsContext, updatedSpec);
        submitterPlugin = new SubmitterPlugin<>(stageName, context, transform, contextProvider,
                                                ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType) || isConnectorSink) {

        BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(stageName, evaluator);
        StageSpec updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator,
                                               batchSink::configurePipeline);
        updatedStageSpecs.add(updatedSpec);
        ContextProvider<SparkBatchSinkContext> contextProvider =
          dsContext -> new SparkBatchSinkContext(sinkFactory, context, pipelineRuntime, dsContext, updatedSpec);
        submitterPlugin = new SubmitterPlugin<>(stageName, context, batchSink, contextProvider,
                                                ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));

      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {

        BatchConfigurable<SparkPluginContext> sparkSink = pluginInstantiator.newPluginInstance(stageName, evaluator);
        StageSpec updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator,
                                               sparkSink::configurePipeline);
        updatedStageSpecs.add(updatedSpec);
        ContextProvider<BasicSparkPluginContext> contextProvider =
          dsContext -> new BasicSparkPluginContext(context, pipelineRuntime, updatedSpec, dsContext, admin);
        submitterPlugin = new SubmitterPlugin<>(stageName, context, sparkSink, contextProvider, ctx -> { });

      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

        BatchAggregator aggregator = pluginInstantiator.newPluginInstance(stageName, evaluator);
        StageSpec updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator,
                                               aggregator::configurePipeline);
        updatedStageSpecs.add(updatedSpec);
        ContextProvider<DefaultAggregatorContext> contextProvider =
          new AggregatorContextProvider(pipelineRuntime, updatedSpec, admin);
        submitterPlugin = new SubmitterPlugin<>(stageName, context, aggregator, contextProvider,
                                                ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));

      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

        BatchJoiner joiner = pluginInstantiator.newPluginInstance(stageName, evaluator);
        StageSpec updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator,
                                               joiner::configurePipeline);
        updatedStageSpecs.add(updatedSpec);
        ContextProvider<DefaultJoinerContext> contextProvider =
          new JoinerContextProvider(pipelineRuntime, updatedSpec, admin);
        submitterPlugin = new SubmitterPlugin<>(stageName, context, joiner, contextProvider, sparkJoinerContext -> {
          stagePartitions.put(stageName, sparkJoinerContext.getNumPartitions());
          stageOperations.put(stageName, sparkJoinerContext.getFieldOperations());
        });

      }
      if (submitterPlugin != null) {
        submitterPlugin.prepareRun();
        finishers.add(submitterPlugin);
      }
    }

    // create a new BatchPhaseSpec based on updated stage specs
    PipelinePhase updatedPhase = new PipelinePhase(updatedStageSpecs, phase.getDag());
    BatchPhaseSpec updatedPhaseSpec = new BatchPhaseSpec(phaseSpec, updatedPhase);
    File configFile = File.createTempFile("HydratorSpark", ".config");
    cleanupFiles.add(configFile);
    try (Writer writer = Files.newBufferedWriter(configFile.toPath(), StandardCharsets.UTF_8)) {
      SparkBatchSourceSinkFactoryInfo sourceSinkInfo = new SparkBatchSourceSinkFactoryInfo(sourceFactory,
                                                                                           sinkFactory,
                                                                                           stagePartitions,
                                                                                           updatedPhaseSpec);
      writer.write(GSON.toJson(sourceSinkInfo));
    }

    finisher = new CompositeFinisher(finishers);
    context.localize("HydratorSpark.config", configFile.toURI());

    WorkflowToken token = context.getWorkflowToken();
    if (token != null) {
      for (Map.Entry<String, String> entry : pipelineRuntime.getArguments().getAddedArguments().entrySet()) {
        token.put(entry.getKey(), entry.getValue());
      }
      // Put the collected field operations in workflow token
      token.put(Constants.FIELD_OPERATION_KEY_IN_WORKFLOW_TOKEN, GSON.toJson(stageOperations));
    }
  }

  private StageSpec configureStage(StageSpec currentSpec, DefaultPipelineConfigurer pipelineConfigurer,
                                   SchemaPropagator schemaPropagator,
                                   Consumer<DefaultPipelineConfigurer> configureConsumer) {
    configureConsumer.accept(pipelineConfigurer);

    DefaultStageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    StageSpec.Builder updatedSpec = StageSpec.builder(currentSpec)
      .setErrorSchema(stageConfigurer.getErrorSchema())
      .addInputSchemas(stageConfigurer.getInputSchemas());
    Map<String, Schema> outputPortSchemas = stageConfigurer.getOutputPortSchemas();
    for (Map.Entry<String, StageSpec.Port> entry: currentSpec.getOutputPorts().entrySet()) {
      String port = entry.getValue().getPort();
      if (port == null) {
        updatedSpec.addOutput(stageConfigurer.getOutputSchema(), entry.getKey());
      } else {
        updatedSpec.addOutput(entry.getKey(), port, outputPortSchemas.get(port));
      }
    }
    StageSpec updated = updatedSpec.build();
    schemaPropagator.propagateSchema(updated);
    return updated;
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void destroy() {
    if (finisher != null) {
      finisher.onFinish(getContext().getState().getStatus() == ProgramStatus.COMPLETED);
    }
    for (File file : cleanupFiles) {
      if (!file.delete()) {
        LOG.warn("Failed to clean up resource {} ", file);
      }
    }
  }
}
