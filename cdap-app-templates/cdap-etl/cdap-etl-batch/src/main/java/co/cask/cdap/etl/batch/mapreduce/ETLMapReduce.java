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
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.DefaultAggregatorContext;
import co.cask.cdap.etl.batch.DefaultJoinerContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.StageFailureException;
import co.cask.cdap.etl.batch.connector.MultiConnectorFactory;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.TypeChecker;
import co.cask.cdap.etl.common.submit.AggregatorContextProvider;
import co.cask.cdap.etl.common.submit.CompositeFinisher;
import co.cask.cdap.etl.common.submit.ContextProvider;
import co.cask.cdap.etl.common.submit.Finisher;
import co.cask.cdap.etl.common.submit.JoinerContextProvider;
import co.cask.cdap.etl.common.submit.SubmitterPlugin;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.SetMultimap;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce Driver for ETL Batch Applications.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  public static final String MAP_KEY_CLASS = "cdap.etl.map.key.class";
  public static final String MAP_VAL_CLASS = "cdap.etl.map.val.class";
  static final String RUNTIME_ARGS_KEY = "cdap.etl.runtime.args";
  static final String INPUT_ALIAS_KEY = "cdap.etl.source.alias.key";
  static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  static final Type INPUT_ALIAS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, SinkOutput>>() { }.getType();
  static final Type CONNECTOR_DATASETS_TYPE = new TypeToken<HashSet<String>>() { }.getType();
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

  private final Set<String> connectorDatasets;

  public ETLMapReduce(BatchPhaseSpec phaseSpec) {
    this(phaseSpec, new HashSet<String>());
  }

  public ETLMapReduce(BatchPhaseSpec phaseSpec, Set<String> connectorDatasets) {
    this.phaseSpec = phaseSpec;
    this.connectorDatasets = connectorDatasets;
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
    Set<StageSpec> reducers = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE,
                                                                   BatchJoiner.PLUGIN_TYPE);
    if (reducers.size() > 1) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' cannot contain more than one reducer but it has reducers '%s'.",
        phaseSpec.getPhaseName(), Joiner.on(',').join(reducers)));
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec));
    properties.put(Constants.CONNECTOR_DATASETS, GSON.toJson(connectorDatasets));
    setProperties(properties);
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void initialize() throws Exception {
    final MapReduceContext context = getContext();
    Map<String, String> properties = context.getSpecification().getProperties();
    if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
      LogStageInjector.start();
    }
    PipelineRuntime pipelineRuntime = new PipelineRuntime(context, mrMetrics);
    List<Finisher> finishers = new ArrayList<>();

    final Job job = context.getHadoopJob();
    final Configuration hConf = job.getConfiguration();
    hConf.setBoolean("mapreduce.map.speculative", false);
    hConf.setBoolean("mapreduce.reduce.speculative", false);

    // plugin name -> runtime args for that plugin
    MacroEvaluator evaluator = new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                                                         context.getLogicalStartTime(),
                                                         context, context.getNamespace());

    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
    Set<String> connectorDatasets = GSON.fromJson(properties.get(Constants.CONNECTOR_DATASETS),
                                                  CONNECTOR_DATASETS_TYPE);

    for (Map.Entry<String, String> pipelineProperty : phaseSpec.getPipelineProperties().entrySet()) {
      hConf.set(pipelineProperty.getKey(), pipelineProperty.getValue());
    }

    final PipelinePhase phase = phaseSpec.getPhase();
    PipelinePluginInstantiator pluginInstantiator =
      new PipelinePluginInstantiator(context, mrMetrics, phaseSpec, new MultiConnectorFactory());

    // should never happen if planner is correct
    Set<StageSpec> reducers = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE,
                                                                   BatchJoiner.PLUGIN_TYPE);
    if (reducers.size() > 1) {
      Iterator<StageSpec> reducerIter = reducers.iterator();
      StringBuilder reducersStr = new StringBuilder(reducerIter.next().getName());
      while (reducerIter.hasNext()) {
        reducersStr.append(",");
        reducersStr.append(reducerIter.next().getName());
      }
      throw new IllegalStateException("Found multiple reducers ( " + reducersStr + " ) in the same pipeline phase. " +
                                        "This means there was a bug in planning the pipeline when it was deployed. ");
    }

    job.setMapperClass(ETLMapper.class);
    if (reducers.isEmpty()) {
      job.setNumReduceTasks(0);
    } else {
      job.setReducerClass(ETLReducer.class);
    }

    final Map<String, SinkOutput> sinkOutputs = new HashMap<>();
    final Map<String, String> inputAliasToStage = new HashMap<>();
    // call prepareRun on each stage in order so that any arguments set by a stage will be visible to subsequent stages
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
        ContextProvider<MapReduceBatchContext> contextProvider =
          new MapReduceBatchContextProvider(context, pipelineRuntime, stageSpec, connectorDatasets);
        submitterPlugin = new SubmitterPlugin<>(
          stageName, context, batchSource, contextProvider,
          new SubmitterPlugin.PrepareAction<MapReduceBatchContext>() {
            @Override
            public void act(MapReduceBatchContext sourceContext) {
              for (String inputAlias : sourceContext.getInputNames()) {
                inputAliasToStage.put(inputAlias, stageName);
              }
            }
          });

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType) ||
        AlertPublisher.PLUGIN_TYPE.equals(pluginType) || isConnectorSink) {

        BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<MapReduceBatchContext> contextProvider =
          new MapReduceBatchContextProvider(context, pipelineRuntime, stageSpec, connectorDatasets);
        submitterPlugin = new SubmitterPlugin<>(
          stageName, context, batchSink, contextProvider,
          new SubmitterPlugin.PrepareAction<MapReduceBatchContext>() {
            @Override
            public void act(MapReduceBatchContext sinkContext) {
              sinkOutputs.put(stageName, new SinkOutput(sinkContext.getOutputNames()));
            }
          });

      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

        Transform<?, ?> transform = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<MapReduceBatchContext> contextProvider =
          new MapReduceBatchContextProvider(context, pipelineRuntime, stageSpec, connectorDatasets);
        submitterPlugin = new SubmitterPlugin<>(stageName, context, transform, contextProvider);

      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

        final BatchAggregator<?, ?, ?> aggregator = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<DefaultAggregatorContext> contextProvider =
          new AggregatorContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
        submitterPlugin = new SubmitterPlugin<>(
          stageName, context, aggregator, contextProvider,
          new SubmitterPlugin.PrepareAction<DefaultAggregatorContext>() {
            @Override
            public void act(DefaultAggregatorContext aggregatorContext) {
              if (aggregatorContext.getNumPartitions() != null) {
                job.setNumReduceTasks(aggregatorContext.getNumPartitions());
              }
              Class<?> outputKeyClass = aggregatorContext.getGroupKeyClass();
              Class<?> outputValClass = aggregatorContext.getGroupValueClass();

              if (outputKeyClass == null) {
                outputKeyClass = TypeChecker.getGroupKeyClass(aggregator);
              }
              if (outputValClass == null) {
                outputValClass = TypeChecker.getGroupValueClass(aggregator);
              }
              hConf.set(MAP_KEY_CLASS, outputKeyClass.getName());
              hConf.set(MAP_VAL_CLASS, outputValClass.getName());
              job.setMapOutputKeyClass(getOutputKeyClass(stageName, outputKeyClass));
              job.setMapOutputValueClass(getOutputValClass(stageName, outputValClass));
            }
          });

      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

        final BatchJoiner<?, ?, ?> batchJoiner = pluginInstantiator.newPluginInstance(stageName, evaluator);
        ContextProvider<DefaultJoinerContext> contextProvider =
          new JoinerContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
        submitterPlugin = new SubmitterPlugin<>(
          stageName, context, batchJoiner, contextProvider,
          new SubmitterPlugin.PrepareAction<DefaultJoinerContext>() {
            @Override
            public void act(DefaultJoinerContext joinerContext) {
              if (joinerContext.getNumPartitions() != null) {
                job.setNumReduceTasks(joinerContext.getNumPartitions());
              }
              Class<?> outputKeyClass = joinerContext.getJoinKeyClass();
              Class<?> inputRecordClass = joinerContext.getJoinInputRecordClass();

              if (outputKeyClass == null) {
                outputKeyClass = TypeChecker.getJoinKeyClass(batchJoiner);
              }
              if (inputRecordClass == null) {
                inputRecordClass = TypeChecker.getJoinInputRecordClass(batchJoiner);
              }
              hConf.set(MAP_KEY_CLASS, outputKeyClass.getName());
              hConf.set(MAP_VAL_CLASS, inputRecordClass.getName());
              job.setMapOutputKeyClass(getOutputKeyClass(stageName, outputKeyClass));
              getOutputValClass(stageName, inputRecordClass);
              // for joiner plugin map output is tagged with stageName
              job.setMapOutputValueClass(TaggedWritable.class);
            }
          });
      }
      if (submitterPlugin != null) {
        submitterPlugin.prepareRun();
        finishers.add(submitterPlugin);
      }
    }

    hConf.set(SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));
    hConf.set(INPUT_ALIAS_KEY, GSON.toJson(inputAliasToStage));
    finisher = new CompositeFinisher(finishers);

    job.setMapperClass(ETLMapper.class);

    WorkflowToken token = context.getWorkflowToken();
    if (token != null) {
      for (Map.Entry<String, String> entry : pipelineRuntime.getArguments().getAddedArguments().entrySet()) {
        token.put(entry.getKey(), entry.getValue());
      }
    }
    // token is null when just the mapreduce job is run but not the entire workflow
    // we still want things to work in that case.
    hConf.set(RUNTIME_ARGS_KEY, GSON.toJson(pipelineRuntime.getArguments().asMap()));
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
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void destroy() {
    boolean isSuccessful = getContext().getState().getStatus() == ProgramStatus.COMPLETED;
    finisher.onFinish(isSuccessful);
    LOG.info("Batch Run finished : status = {}", getContext().getState());
  }

  /**
   * Provider for MapReduceBatchContexts.
   */
  private static class MapReduceBatchContextProvider implements ContextProvider<MapReduceBatchContext> {
    private final MapReduceContext context;
    private final PipelineRuntime pipelineRuntime;
    private final StageSpec stageSpec;
    private final Set<String> connectorDatasets;

    private MapReduceBatchContextProvider(MapReduceContext context, PipelineRuntime pipelineRuntime, StageSpec
      stageSpec, Set<String> connectorDatasets) {
      this.context = context;
      this.pipelineRuntime = pipelineRuntime;
      this.stageSpec = stageSpec;
      this.connectorDatasets = connectorDatasets;
    }

    @Override
    public MapReduceBatchContext getContext(DatasetContext datasetContext) {
      return new MapReduceBatchContext(context, pipelineRuntime, stageSpec, connectorDatasets, datasetContext);
    }
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
      } catch (StageFailureException e) {
        PIPELINE_LOG.error("{}", e.getMessage(), e.getCause());
        Throwables.propagate(e.getCause());
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
      } catch (StageFailureException e) {
        PIPELINE_LOG.error("{}", e.getMessage(), e.getCause());
        Throwables.propagate(e.getCause());
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
