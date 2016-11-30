/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.batch.AbstractAggregatorContext;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.common.CompositeFinisher;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.Finisher;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.planner.StageInfo;
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

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec, BatchPhaseSpec.class));
    setProperties(properties);
  }

  @Override
  public void initialize() throws Exception {
    SparkClientContext context = getContext();
    cleanupFiles = new ArrayList<>();
    CompositeFinisher.Builder finishers = CompositeFinisher.builder();

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=256m");
    sparkConf.set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m");
    context.setSparkConf(sparkConf);

    Map<String, String> properties = context.getSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
    DatasetContextLookupProvider lookProvider = new DatasetContextLookupProvider(context);
    MacroEvaluator evaluator = new DefaultMacroEvaluator(context.getWorkflowToken(), context.getRuntimeArguments(),
                                                         context.getLogicalStartTime(), context,
                                                         context.getNamespace());
    SparkBatchSourceFactory sourceFactory = new SparkBatchSourceFactory();
    SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    Map<String, Integer> stagePartitions = new HashMap<>();

    for (StageInfo stageInfo : phaseSpec.getPhase()) {
      String stageName = stageInfo.getName();
      String pluginType = stageInfo.getPluginType();

      if (BatchSource.PLUGIN_TYPE.equals(pluginType)) {
        BatchConfigurable<BatchSourceContext> batchSource = context.newPluginInstance(stageName, evaluator);
        BatchSourceContext sourceContext = new SparkBatchSourceContext(sourceFactory, context, lookProvider, stageName);
        batchSource.prepareRun(sourceContext);
        finishers.add(batchSource, sourceContext);
      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType)) {
        BatchConfigurable<BatchSinkContext> batchSink = context.newPluginInstance(stageName, evaluator);
        BatchSinkContext sinkContext = new SparkBatchSinkContext(sinkFactory, context, null, stageName);
        batchSink.prepareRun(sinkContext);
        finishers.add(batchSink, sinkContext);
      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {
        BatchConfigurable<SparkPluginContext> sparkSink = context.newPluginInstance(stageName, evaluator);
        SparkPluginContext sparkPluginContext = new BasicSparkPluginContext(context, lookProvider, stageName);
        sparkSink.prepareRun(sparkPluginContext);
        finishers.add(sparkSink, sparkPluginContext);
      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
        BatchAggregator aggregator = context.newPluginInstance(stageName, evaluator);
        AbstractAggregatorContext aggregatorContext =
          new SparkAggregatorContext(context, new DatasetContextLookupProvider(context), stageName);
        aggregator.prepareRun(aggregatorContext);
        finishers.add(aggregator, aggregatorContext);
        stagePartitions.put(stageName, aggregatorContext.getNumPartitions());
      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
        BatchJoiner joiner = context.newPluginInstance(stageName, evaluator);
        SparkJoinerContext sparkJoinerContext = new SparkJoinerContext(stageName, context);
        joiner.prepareRun(sparkJoinerContext);
        finishers.add(joiner, sparkJoinerContext);
        stagePartitions.put(stageName, sparkJoinerContext.getNumPartitions());
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

    finisher = finishers.build();
    context.localize("HydratorSpark.config", configFile.toURI());
  }

  @Override
  public void destroy() {
    finisher.onFinish(getContext().getState().getStatus() == ProgramStatus.COMPLETED);
    for (File file : cleanupFiles) {
      if (!file.delete()) {
        LOG.warn("Failed to clean up resource {} ", file);
      }
    }
  }

}
