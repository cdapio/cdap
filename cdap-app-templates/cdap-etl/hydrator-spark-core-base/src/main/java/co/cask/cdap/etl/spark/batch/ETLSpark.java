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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.StageSubmitterContext;
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
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.DefaultAggregatorContext;
import co.cask.cdap.etl.batch.DefaultJoinerContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.connector.SingleConnectorFactory;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.submit.AggregatorContextProvider;
import co.cask.cdap.etl.common.submit.CompositeFinisher;
import co.cask.cdap.etl.common.submit.ContextProvider;
import co.cask.cdap.etl.common.submit.Finisher;
import co.cask.cdap.etl.common.submit.JoinerContextProvider;
import co.cask.cdap.etl.common.submit.SubmitterPlugin;
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import co.cask.cdap.etl.spec.StageSpec;
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
import java.util.List;
import java.util.Map;

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
    final SparkClientContext context = getContext();
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

    MacroEvaluator evaluator = new DefaultMacroEvaluator(new BasicArguments(context),
                                                         context.getLogicalStartTime(), context,
                                                         context.getNamespace());
    final SparkBatchSourceFactory sourceFactory = new SparkBatchSourceFactory();
    final SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    final Map<String, Integer> stagePartitions = new HashMap<>();
    PluginContext pluginContext = new SparkPipelinePluginContext(context, context.getMetrics(),
                                                                 phaseSpec.isStageLoggingEnabled(),
                                                                 phaseSpec.isProcessTimingEnabled());
    PipelinePluginInstantiator pluginInstantiator =
      new PipelinePluginInstantiator(pluginContext, context.getMetrics(), phaseSpec, new SingleConnectorFactory());
    final PipelineRuntime pipelineRuntime = new PipelineRuntime(context);
    final Admin admin = context.getAdmin();

    PipelinePhase phase = phaseSpec.getPhase();
    // go through in topological order so that arguments set by one stage are seen by stages after it
    for (final String stageName : phase.getDag().getTopologicalOrder()) {
      final StageSpec stageSpec = phase.getStage(stageName);
      String pluginType = stageSpec.getPluginType();
      boolean isConnectorSource =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSources().contains(stageName);
      boolean isConnectorSink =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSinks().contains(stageName);

      SubmitterPlugin submitterPlugin = null;
      if (BatchSource.PLUGIN_TYPE.equals(pluginType) || isConnectorSource) {

        BatchConfigurable<BatchSourceContext> batchSource = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<BatchSourceContext> contextProvider =
          new ContextProvider<BatchSourceContext>() {
            @Override
            public BatchSourceContext getContext(DatasetContext datasetContext) {
              return new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, datasetContext, stageSpec);
            }
          };
        submitterPlugin = new SubmitterPlugin(stageName, context, batchSource, contextProvider);

      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

        Transform transform = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<StageSubmitterContext> contextProvider =
          new ContextProvider<StageSubmitterContext>() {
            @Override
            public StageSubmitterContext getContext(DatasetContext datasetContext) {
              return new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, datasetContext, stageSpec);
            }
          };
        submitterPlugin = new SubmitterPlugin(stageName, context, transform, contextProvider);

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType) || isConnectorSink) {

        BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<BatchSinkContext> contextProvider = new ContextProvider<BatchSinkContext>() {
          @Override
          public BatchSinkContext getContext(DatasetContext datasetContext) {
            return new SparkBatchSinkContext(sinkFactory, context, pipelineRuntime, datasetContext, stageSpec);
          }
        };
        submitterPlugin = new SubmitterPlugin(stageName, context, batchSink, contextProvider);

      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {

        BatchConfigurable<SparkPluginContext> sparkSink = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<SparkPluginContext> contextProvider =
          new ContextProvider<SparkPluginContext>() {
            @Override
            public SparkPluginContext getContext(DatasetContext datasetContext) {
              return new BasicSparkPluginContext(context, pipelineRuntime, stageSpec, datasetContext, admin);
            }
          };
        submitterPlugin = new SubmitterPlugin(stageName, context, sparkSink, contextProvider);

      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

        BatchAggregator aggregator = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<DefaultAggregatorContext> contextProvider =
          new AggregatorContextProvider(pipelineRuntime, stageSpec, admin);
        submitterPlugin = new SubmitterPlugin(stageName, context, aggregator, contextProvider);

      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

        BatchJoiner joiner = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<DefaultJoinerContext> contextProvider =
          new JoinerContextProvider(pipelineRuntime, stageSpec, admin);
        submitterPlugin = new SubmitterPlugin<>(
          stageName, context, joiner, contextProvider,
          new SubmitterPlugin.PrepareAction<DefaultJoinerContext>() {
            @Override
            public void act(DefaultJoinerContext sparkJoinerContext) {
              stagePartitions.put(stageName, sparkJoinerContext.getNumPartitions());
            }
          });

      }
      if (submitterPlugin != null) {
        submitterPlugin.prepareRun();
        finishers.add(submitterPlugin);
      }
    }

    File configFile = File.createTempFile("HydratorSpark", ".config");
    cleanupFiles.add(configFile);
    try (Writer writer = Files.newBufferedWriter(configFile.toPath(), StandardCharsets.UTF_8)) {
      SparkBatchSourceSinkFactoryInfo sourceSinkInfo = new SparkBatchSourceSinkFactoryInfo(sourceFactory,
                                                                                           sinkFactory,
                                                                                           stagePartitions);
      writer.write(GSON.toJson(sourceSinkInfo));
    }

    finisher = new CompositeFinisher(finishers);
    context.localize("HydratorSpark.config", configFile.toURI());

    WorkflowToken token = context.getWorkflowToken();
    if (token != null) {
      for (Map.Entry<String, String> entry : pipelineRuntime.getArguments().getAddedArguments().entrySet()) {
        token.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void destroy() {
    finisher.onFinish(getContext().getState().getStatus() == ProgramStatus.COMPLETED);
    for (File file : cleanupFiles) {
      if (!file.delete()) {
        LOG.warn("Failed to clean up resource {} ", file);
      }
    }
  }

}
