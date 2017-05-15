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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.DefaultAggregatorContext;
import co.cask.cdap.etl.batch.DefaultJoinerContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.common.CompositeFinisher;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.Finisher;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.TypeChecker;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
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
  static final String INPUT_ALIAS_KEY = "cdap.etl.source.alias.key";
  static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  static final String MAP_KEY_CLASS = "cdap.etl.map.key.class";
  static final String MAP_VAL_CLASS = "cdap.etl.map.val.class";
  static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, Map<String, String>>>() { }.getType();
  static final Type INPUT_ALIAS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, SinkOutput>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Logger PIPELINE_LOG = new LocationAwareMDCWrapperLogger(LOG, Constants.EVENT_TYPE_TAG,
                                                                              Constants.PIPELINE_LIFECYCLE_TAG_VALUE);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .create();

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
    setDescription("MapReduce phase executor. " + phaseSpec.getDescription());

    // Set resources for mapper, reducer and driver
    setMapperResources(phaseSpec.getResources());
    setReducerResources(phaseSpec.getResources());
    setDriverResources(phaseSpec.getDriverResources());

    Set<String> sources = phaseSpec.getPhase().getSources();
    // Planner should make sure this never happens
    if (sources.isEmpty()) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' must contain at least one source but it has no sources.", phaseSpec.getPhaseName()));
    }
    if (phaseSpec.getPhase().getSinks().isEmpty()) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' must contain at least one sink but does not have any.", phaseSpec.getPhaseName()));
    }
    Set<StageInfo> reducers = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE,
                                                                   BatchJoiner.PLUGIN_TYPE);
    if (reducers.size() > 1) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' cannot contain more than one reducer but it has reducers '%s'.",
        phaseSpec.getPhaseName(), Joiner.on(',').join(reducers)));
    } else if (!reducers.isEmpty()) {
      String reducerName = reducers.iterator().next().getName();
      PipelinePhase mapperPipeline = phaseSpec.getPhase().subsetTo(ImmutableSet.of(reducerName));
      for (StageInfo stageInfo : mapperPipeline) {
        // error datasets are not supported in the map phase of a mapreduce, because we can only
        // write out the group/join key and not error dataset data.
        // we need to re-think how error datasets are done. perhaps they are just sinks instead of a special thing.
        if (stageInfo.getErrorDatasetName() != null) {
          throw new IllegalArgumentException(String.format(
            "Stage %s is not allowed to have an error dataset because it connects to reducer %s.",
            stageInfo.getName(), reducerName));
        }
      }
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec));
    setProperties(properties);
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    Map<String, String> properties = context.getSpecification().getProperties();
    if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
      LogStageInjector.start();
    }
    CompositeFinisher.Builder finishers = CompositeFinisher.builder();

    Job job = context.getHadoopJob();
    Configuration hConf = job.getConfiguration();
    hConf.setBoolean("mapreduce.map.speculative", false);
    hConf.setBoolean("mapreduce.reduce.speculative", false);

    // plugin name -> runtime args for that plugin
    Map<String, Map<String, String>> runtimeArgs = new HashMap<>();
    MacroEvaluator evaluator = new DefaultMacroEvaluator(context.getWorkflowToken(), context.getRuntimeArguments(),
                                                         context.getLogicalStartTime(),
                                                         context, context.getNamespace());

    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);

    for (Map.Entry<String, String> pipelineProperty : phaseSpec.getPipelineProperties().entrySet()) {
      hConf.set(pipelineProperty.getKey(), pipelineProperty.getValue());
    }

    PipelinePhase phase = phaseSpec.getPhase();
    PipelinePluginInstantiator pluginInstantiator = new PipelinePluginInstantiator(context, mrMetrics, phaseSpec);

    Map<String, String> inputAliasToStage = new HashMap<>();
    for (String sourceName : phaseSpec.getPhase().getSources()) {
      try {
        BatchConfigurable<BatchSourceContext> batchSource = pluginInstantiator.newPluginInstance(sourceName, evaluator);
        StageInfo stageInfo = phaseSpec.getPhase().getStage(sourceName);
        MapReduceBatchContext sourceContext = new MapReduceBatchContext(context, mrMetrics, stageInfo);
        batchSource.prepareRun(sourceContext);
        runtimeArgs.put(sourceName, sourceContext.getRuntimeArguments());
        for (String inputAlias : sourceContext.getInputNames()) {
          inputAliasToStage.put(inputAlias, sourceName);
        }
        finishers.add(batchSource, sourceContext);
      } catch (Exception e) {
        // Catch the Exception to generate a User Error Log for the Pipeline
        PIPELINE_LOG.error("Failed to initialize batch source '{}' with the error: {}. Please review your pipeline " +
                            "configuration and check the system logs for more details.", sourceName,
                          Throwables.getRootCause(e).getMessage(), Throwables.getRootCause(e));
        throw e;
      }
    }
    hConf.set(INPUT_ALIAS_KEY, GSON.toJson(inputAliasToStage));

    Map<String, SinkOutput> sinkOutputs = new HashMap<>();
    for (StageInfo stageInfo : Sets.union(phase.getStagesOfType(Constants.CONNECTOR_TYPE),
                                          phase.getStagesOfType(BatchSink.PLUGIN_TYPE))) {
      String sinkName = stageInfo.getName();
      // todo: add a better way to get info for all sinks
      if (!phase.getSinks().contains(sinkName)) {
        continue;
      }
      try {
        BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(sinkName, evaluator);
        MapReduceBatchContext sinkContext = new MapReduceBatchContext(context, mrMetrics, stageInfo);
        batchSink.prepareRun(sinkContext);
        runtimeArgs.put(sinkName, sinkContext.getRuntimeArguments());
        finishers.add(batchSink, sinkContext);
        sinkOutputs.put(sinkName, new SinkOutput(sinkContext.getOutputNames(), stageInfo.getErrorDatasetName()));
      } catch (Exception e) {
        // Catch the Exception to generate a User Error Log for the Pipeline
        PIPELINE_LOG.error("Failed to initialize batch sink '{}' with the error: {}. Please review your pipeline " +
                            "configuration and check the system logs for more details.", sinkName,
                          Throwables.getRootCause(e).getMessage(), Throwables.getRootCause(e));
        throw e;
      }
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
        context.addOutput(Output.ofDataset(stageInfo.getErrorDatasetName(), args));
      }
    }
    job.setMapperClass(ETLMapper.class);

    Set<StageInfo> reducers = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE,
                                                                   BatchJoiner.PLUGIN_TYPE);
    if (!reducers.isEmpty()) {
      job.setReducerClass(ETLReducer.class);
      String reducerName = reducers.iterator().next().getName();
      StageInfo stageInfo = phase.getStage(reducerName);
      Class<?> outputKeyClass;
      Class<?> outputValClass;
      try {
        if (!phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE).isEmpty()) {
          BatchAggregator aggregator = pluginInstantiator.newPluginInstance(reducerName, evaluator);
          DefaultAggregatorContext aggregatorContext = new DefaultAggregatorContext(context, mrMetrics, stageInfo);
          aggregator.prepareRun(aggregatorContext);
          finishers.add(aggregator, aggregatorContext);

          if (aggregatorContext.getNumPartitions() != null) {
            job.setNumReduceTasks(aggregatorContext.getNumPartitions());
          }
          outputKeyClass = aggregatorContext.getGroupKeyClass();
          outputValClass = aggregatorContext.getGroupValueClass();

          if (outputKeyClass == null) {
            outputKeyClass = TypeChecker.getGroupKeyClass(aggregator);
          }
          if (outputValClass == null) {
            outputValClass = TypeChecker.getGroupValueClass(aggregator);
          }
          hConf.set(MAP_KEY_CLASS, outputKeyClass.getName());
          hConf.set(MAP_VAL_CLASS, outputValClass.getName());
          job.setMapOutputKeyClass(getOutputKeyClass(reducerName, outputKeyClass));
          job.setMapOutputValueClass(getOutputValClass(reducerName, outputValClass));
        } else { // reducer type is joiner
          BatchJoiner batchJoiner = pluginInstantiator.newPluginInstance(reducerName, evaluator);
          DefaultJoinerContext joinerContext = new DefaultJoinerContext(context, mrMetrics, stageInfo);
          batchJoiner.prepareRun(joinerContext);
          finishers.add(batchJoiner, joinerContext);

          if (joinerContext.getNumPartitions() != null) {
            job.setNumReduceTasks(joinerContext.getNumPartitions());
          }
          outputKeyClass = joinerContext.getJoinKeyClass();
          Class<?> inputRecordClass = joinerContext.getJoinInputRecordClass();

          if (outputKeyClass == null) {
            outputKeyClass = TypeChecker.getJoinKeyClass(batchJoiner);
          }
          if (inputRecordClass == null) {
            inputRecordClass = TypeChecker.getJoinInputRecordClass(batchJoiner);
          }
          hConf.set(MAP_KEY_CLASS, outputKeyClass.getName());
          hConf.set(MAP_VAL_CLASS, inputRecordClass.getName());
          job.setMapOutputKeyClass(getOutputKeyClass(reducerName, outputKeyClass));
          getOutputValClass(reducerName, inputRecordClass);
          // for joiner plugin map output is tagged with stageName
          job.setMapOutputValueClass(TaggedWritable.class);
        }
      } catch (Exception e) {
        // Catch the Exception to generate a User Error Log for the Pipeline
        PIPELINE_LOG.error("Failed to initialize pipeline stage '{}' with the error: {}. Please review your pipeline " +
                            "configuration and check the system logs for more details.", reducerName,
                          Throwables.getRootCause(e).getMessage(), Throwables.getRootCause(e));
        throw e;
      }
    } else {
      job.setNumReduceTasks(0);
    }

    hConf.set(RUNTIME_ARGS_KEY, GSON.toJson(runtimeArgs));
  }

  private Class<?> getOutputKeyClass(String reducerName, Class<?> outputKeyClass) {
    // in case the classes are not a WritableComparable, but is some common type we support
    // for example, a String or a StructuredRecord
    WritableConversion writableConversion = WritableConversions.getConversion(outputKeyClass.getName());
    // if the conversion is null, it means the user is using their own object.
    if (writableConversion != null) {
      outputKeyClass = writableConversion.getWritableClass();
    }
    // check classes here instead of letting mapreduce do it, since mapreduce throws a cryptic error
    if (!WritableComparable.class.isAssignableFrom(outputKeyClass)) {
      throw new IllegalArgumentException(String.format(
        "Invalid reducer %s. The key class %s must implement Hadoop's WritableComparable.",
        reducerName, outputKeyClass));
    }
    return outputKeyClass;
  }

  private Class<?> getOutputValClass(String reducerName, Class<?> outputValClass) {
    WritableConversion writableConversion;
    writableConversion = WritableConversions.getConversion(outputValClass.getName());
    if (writableConversion != null) {
      outputValClass = writableConversion.getWritableClass();
    }
    if (!Writable.class.isAssignableFrom(outputValClass)) {
      throw new IllegalArgumentException(String.format(
        "Invalid reducer %s. The value class %s must implement Hadoop's Writable.",
        reducerName, outputValClass));
    }
    return outputValClass;
  }

  @Override
  public void destroy() {
    boolean isSuccessful = getContext().getState().getStatus() == ProgramStatus.COMPLETED;
    finisher.onFinish(isSuccessful);
    LOG.info("Batch Run finished : status = {}", getContext().getState());
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
