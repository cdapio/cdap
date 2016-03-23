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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.CompositeFinisher;
import co.cask.cdap.etl.batch.Finisher;
import co.cask.cdap.etl.batch.LoggedBatchConfigurable;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.TypeChecker;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce Driver for ETL Batch Applications.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  static final String RUNTIME_ARGS_KEY = "cdap.etl.runtime.args";
  static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  static final String GROUP_KEY_CLASS = "cdap.etl.aggregator.group.key.class";
  static final String GROUP_VAL_CLASS = "cdap.etl.aggregator.group.val.class";
  static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, Map<String, String>>>() { }.getType();
  static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, SinkOutput>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>()).create();

  private Finisher finisher;

  // injected by CDAP
  @SuppressWarnings("unused")
  private Metrics mrMetrics;

  // this is only visible at configure time, not at runtime
  private final BatchPhaseSpec phaseSpec;

  public ETLMapReduce(BatchPhaseSpec phaseSpec) {
    this.phaseSpec = phaseSpec;
  }

  @Override
  public void configure() {
    setName(phaseSpec.getPhaseName());

    StringBuilder description = new StringBuilder("DataFlow MapReduce phase executor. Sources");

    for (String sourceName : phaseSpec.getPhase().getSources()) {
      description.append(" '").append(sourceName).append("'");
    }
    description.append(" to sinks");

    for (StageInfo sinkInfo : phaseSpec.getPhase().getStagesOfType(BatchSink.PLUGIN_TYPE)) {
      description.append(" '").append(sinkInfo.getName()).append("'");
    }
    setDescription(description.toString());

    setMapperResources(phaseSpec.getResources());
    setDriverResources(phaseSpec.getResources());

    // These should never happen unless there is a bug in the planner. The planner is supposed to ensure this.
    Set<String> sources = phaseSpec.getPhase().getSources();
    if (sources.size() != 1) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' must contain exactly one source but it has sources '%s'.",
        phaseSpec.getPhaseName(), Joiner.on(',').join(sources)));
    }
    if (phaseSpec.getPhase().getSinks().isEmpty()) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' must contain at least one sink but does not have any.", phaseSpec.getPhaseName()));
    }
    Set<StageInfo> aggregators = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE);
    if (aggregators.size() > 1) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' cannot contain more than one aggregator but it has aggregators '%s'.",
        phaseSpec.getPhaseName(), Joiner.on(',').join(aggregators)));
    } else if (!aggregators.isEmpty()) {
      String aggregatorName = aggregators.iterator().next().getName();
      PipelinePhase mapperPipeline = phaseSpec.getPhase().subsetTo(ImmutableSet.of(aggregatorName));
      for (StageInfo stageInfo : mapperPipeline) {
        // error datasets are not supported in the map phase of a mapreduce, because we can only
        // write out the group key and not error dataset data.
        // we need to re-think how error datasets are done. perhaps they are just sinks instead of a special thing.
        if (stageInfo.getErrorDatasetName() != null) {
          throw new IllegalArgumentException(String.format(
            "Stage %s is not allowed to have an error dataset because it connects to aggregator %s.",
            stageInfo.getName(), aggregatorName));
        }
      }
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec));
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
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
    PipelinePhase phase = phaseSpec.getPhase();
    PipelinePluginInstantiator pluginInstantiator = new PipelinePluginInstantiator(context, phaseSpec);

    // we checked at configure time that there is exactly one source
    String sourceName = phaseSpec.getPhase().getSources().iterator().next();

    BatchConfigurable<BatchSourceContext> batchSource = pluginInstantiator.newPluginInstance(sourceName);
    batchSource = new LoggedBatchConfigurable<>(sourceName, batchSource);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics,
                                                                  new DatasetContextLookupProvider(context),
                                                                  sourceName, context.getRuntimeArguments());
    batchSource.prepareRun(sourceContext);
    runtimeArgs.put(sourceName, sourceContext.getRuntimeArguments());
    finishers.add(batchSource, sourceContext);

    Map<String, SinkOutput> sinkOutputs = new HashMap<>();

    for (StageInfo stageInfo : Sets.union(phase.getStagesOfType(Constants.CONNECTOR_TYPE),
                                          phase.getStagesOfType(BatchSink.PLUGIN_TYPE))) {
      String sinkName = stageInfo.getName();
      // todo: add a better way to get info for all sinks
      if (!phase.getSinks().contains(sinkName)) {
        continue;
      }

      BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(sinkName);
      batchSink = new LoggedBatchConfigurable<>(sinkName, batchSink);
      MapReduceSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics,
                                                                  new DatasetContextLookupProvider(context),
                                                                  sinkName, context.getRuntimeArguments());
      batchSink.prepareRun(sinkContext);
      runtimeArgs.put(sinkName, sinkContext.getRuntimeArguments());
      finishers.add(batchSink, sinkContext);

      sinkOutputs.put(sinkName, new SinkOutput(sinkContext.getOutputNames(), stageInfo.getErrorDatasetName()));
    }
    finisher = finishers.build();
    hConf.set(SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));

    // setup time partition for each error dataset
    for (StageInfo stageInfo : Sets.union(phase.getStagesOfType(Transform.PLUGIN_TYPE),
                                          phase.getStagesOfType(BatchSink.PLUGIN_TYPE))) {
      if (stageInfo.getErrorDatasetName() != null) {
        Map<String, String> args = new HashMap<>();
        args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "avro.schema.output.key",
                 Constants.ERROR_SCHEMA.toString());
        TimePartitionedFileSetArguments.setOutputPartitionTime(args, context.getLogicalStartTime());
        context.addOutput(stageInfo.getErrorDatasetName(), args);
      }
    }

    job.setMapperClass(ETLMapper.class);
    Set<StageInfo> aggregators = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE);
    if (!aggregators.isEmpty()) {
      job.setReducerClass(ETLReducer.class);
      String aggregatorName = aggregators.iterator().next().getName();
      BatchAggregator aggregator = pluginInstantiator.newPluginInstance(aggregatorName);
      MapReduceAggregatorContext aggregatorContext =
        new MapReduceAggregatorContext(context, mrMetrics,
                                       new DatasetContextLookupProvider(context),
                                       aggregatorName, context.getRuntimeArguments());
      aggregator.prepareRun(aggregatorContext);
      finishers.add(aggregator, aggregatorContext);

      if (aggregatorContext.getNumPartitions() != null) {
        job.setNumReduceTasks(aggregatorContext.getNumPartitions());
      }
      // if the plugin sets the output key and value class directly, trust them
      Class<?> outputKeyClass = aggregatorContext.getGroupKeyClass();
      Class<?> outputValClass = aggregatorContext.getGroupValueClass();
      // otherwise, derive it from the plugin's parameters
      if (outputKeyClass == null) {
        outputKeyClass = TypeChecker.getGroupKeyClass(aggregator);
      }
      if (outputValClass == null) {
        outputValClass = TypeChecker.getGroupValueClass(aggregator);
      }
      hConf.set(GROUP_KEY_CLASS, outputKeyClass.getName());
      hConf.set(GROUP_VAL_CLASS, outputValClass.getName());
      // in case the classes are not a WritableComparable, but is some common type we support
      // for example, a String or a StructuredRecord
      WritableConversion writableConversion = WritableConversions.getConversion(outputKeyClass.getName());
      // if the conversion is null, it means the user is using their own object.
      if (writableConversion != null) {
        outputKeyClass = writableConversion.getWritableClass();
      }
      writableConversion = WritableConversions.getConversion(outputValClass.getName());
      if (writableConversion != null) {
        outputValClass = writableConversion.getWritableClass();
      }
      // check classes here instead of letting mapreduce do it, since mapreduce throws a cryptic error
      if (!WritableComparable.class.isAssignableFrom(outputKeyClass)) {
        throw new IllegalArgumentException(String.format(
          "Invalid aggregator %s. The group key class %s must implement Hadoop's WritableComparable.",
          aggregatorName, outputKeyClass));
      }
      if (!Writable.class.isAssignableFrom(outputValClass)) {
        throw new IllegalArgumentException(String.format(
          "Invalid aggregator %s. The group value class %s must implement Hadoop's Writable.",
          aggregatorName, outputValClass));
      }

      job.setMapOutputKeyClass(outputKeyClass);
      job.setMapOutputValueClass(outputValClass);
    } else {
      job.setNumReduceTasks(0);
    }

    hConf.set(RUNTIME_ARGS_KEY, GSON.toJson(runtimeArgs));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    finisher.onFinish(succeeded);
    LOG.info("Batch Run finished : succeeded = {}", succeeded);
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {

    private TransformRunner<Object, Object> transformRunner;
    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics mapperMetrics;

    @Override
    public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {
      // get source, transform, sink ids from program properties
      Map<String, String> properties = context.getSpecification().getProperties();
      if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
        LogStageInjector.start();
      }
      transformRunner = new TransformRunner<>(context, mapperMetrics);
    }

    @Override
    public void map(Object key, Object value, Mapper.Context context) throws IOException, InterruptedException {
      try {
        transformRunner.transform(key, value);
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      transformRunner.destroy();
    }
  }

  /**
   * Reducer for a phase of an ETL pipeline.
   */
  public static class ETLReducer extends Reducer implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {

    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics reducerMetrics;
    private TransformRunner<Object, Iterator> transformRunner;

    @Override
    public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {
      // get source, transform, sink ids from program properties
      Map<String, String> properties = context.getSpecification().getProperties();
      if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
        LogStageInjector.start();
      }
      transformRunner = new TransformRunner<>(context, reducerMetrics);
    }

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
      try {
        transformRunner.transform(key, values.iterator());
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      transformRunner.destroy();
    }
  }
}
