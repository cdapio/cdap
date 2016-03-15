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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.CompositeFinisher;
import co.cask.cdap.etl.batch.Finisher;
import co.cask.cdap.etl.batch.LoggedBatchConfigurable;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRegisterer;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.proto.v1.ETLBatchConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * MapReduce Driver for ETL Batch Applications.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Gson GSON = new Gson();

  private Finisher finisher;

  // injected by CDAP
  @SuppressWarnings("unused")
  private Metrics mrMetrics;

  // this is only visible at configure time, not at runtime
  private final ETLBatchConfig config;

  public ETLMapReduce(ETLBatchConfig config) {
    this.config = config;
  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("MapReduce Driver for ETL Batch Applications");

    PipelineRegisterer pipelineRegisterer = new PipelineRegisterer(getConfigurer(), "batch");

    PipelinePhase pipeline =
      pipelineRegisterer.registerPlugins(
        config, TimePartitionedFileSet.class,
        FileSetProperties.builder()
          .setInputFormat(AvroKeyInputFormat.class)
          .setOutputFormat(AvroKeyOutputFormat.class)
          .setEnableExploreOnCreate(true)
          .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
          .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
          .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
          .setTableProperty("avro.schema.literal", Constants.ERROR_SCHEMA.toString())
          .build(), true);

    Resources resources = config.getResources();
    if (resources != null) {
      setMapperResources(resources);
    }
    Resources driverResources = config.getDriverResources();
    if (driverResources != null) {
      setDriverResources(driverResources);
    }

    // following should never happen
    Preconditions.checkNotNull(pipeline, "Pipeline is null");
    Preconditions.checkNotNull(pipeline.getSinks(), "Sinks could not be found in program properties");
    // empty transform list is created during pipeline register
    Preconditions.checkNotNull(pipeline.getTransforms());
    Preconditions.checkNotNull(pipeline.getConnections(), "Connections could not be found in program properties");

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(pipeline));
    properties.put(Constants.STAGE_LOGGING_ENABLED, String.valueOf(config.isStageLoggingEnabled()));
    if (config.getAggregator() != null) {
      properties.put(Constants.AGGREGATOR_CONFIG, GSON.toJson(config.getAggregator().getPlugin().getProperties()));
    }
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    if (Boolean.valueOf(context.getSpecification().getProperty(Constants.STAGE_LOGGING_ENABLED))) {
      LogStageInjector.start();
    }
    CompositeFinisher.Builder finishers = CompositeFinisher.builder();
    Job job = context.getHadoopJob();
    Configuration hConf = job.getConfiguration();

    // plugin name -> runtime args for that plugin
    Map<String, Map<String, String>> runtimeArgs = new HashMap<>();

    Map<String, String> properties = context.getSpecification().getProperties();
    PipelinePhase pipeline = GSON.fromJson(properties.get(Constants.PIPELINEID), PipelinePhase.class);
    String sourcePluginId = pipeline.getSource().getName();

    BatchConfigurable<BatchSourceContext> batchSource = context.newPluginInstance(sourcePluginId);
    batchSource = new LoggedBatchConfigurable<>(sourcePluginId, batchSource);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics,
                                                                  new DatasetContextLookupProvider(context),
                                                                  sourcePluginId, context.getRuntimeArguments());
    batchSource.prepareRun(sourceContext);
    runtimeArgs.put(sourcePluginId, sourceContext.getRuntimeArguments());
    finishers.add(batchSource, sourceContext);

    Map<String, SinkOutput> sinkOutputs = new HashMap<>();

    for (StageInfo sinkInfo : pipeline.getSinks()) {
      String sinkName = sinkInfo.getName();
      BatchConfigurable<BatchSinkContext> batchSink = context.newPluginInstance(sinkName);
      batchSink = new LoggedBatchConfigurable<>(sinkName, batchSink);
      MapReduceSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics,
                                                                  new DatasetContextLookupProvider(context),
                                                                  sinkName, context.getRuntimeArguments());
      batchSink.prepareRun(sinkContext);
      runtimeArgs.put(sinkName, sinkContext.getRuntimeArguments());
      finishers.add(batchSink, sinkContext);
      sinkOutputs.put(sinkName, new SinkOutput(sinkContext.getOutputNames(), sinkInfo.getErrorDatasetName()));
    }
    finisher = finishers.build();
    hConf.set(ETLMapper.SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));

    // setup time partition for each error dataset
    for (StageInfo stageInfo : Sets.union(pipeline.getTransforms(), pipeline.getSinks())) {
      if (stageInfo.getErrorDatasetName() != null) {
        Map<String, String> args = new HashMap<>();
        args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "avro.schema.output.key",
                 Constants.ERROR_SCHEMA.toString());
        TimePartitionedFileSetArguments.setOutputPartitionTime(args, context.getLogicalStartTime());
        context.addOutput(stageInfo.getErrorDatasetName(), args);
      }
    }

    if (pipeline.getAggregator() != null) {
      hConf.set(Constants.AGGREGATOR_CONFIG, properties.get(Constants.AGGREGATOR_CONFIG));
      job.setMapperClass(ETLAggregatingMapper.class);
      job.setReducerClass(ETLAggregatingReducer.class);
      job.setMapOutputKeyClass(SinkAndStructuredRecord.class);
      job.setMapOutputValueClass(SinkAndStructuredRecord.class);
    } else {
      job.setMapperClass(ETLMapper.class);
      job.setNumReduceTasks(0);
    }

    hConf.set(ETLMapper.RUNTIME_ARGS_KEY, GSON.toJson(runtimeArgs));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    finisher.onFinish(succeeded);
    LOG.info("Batch Run finished : succeeded = {}", succeeded);
  }
}
