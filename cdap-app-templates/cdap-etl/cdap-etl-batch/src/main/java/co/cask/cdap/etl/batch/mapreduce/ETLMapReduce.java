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
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Condition;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.LoggedBatchConfigurable;
import co.cask.cdap.etl.batch.LoggedBatchSink;
import co.cask.cdap.etl.batch.LoggedBatchSource;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.LoggedTransform;
import co.cask.cdap.etl.common.Pipeline;
import co.cask.cdap.etl.common.PipelineRegisterer;
import co.cask.cdap.etl.common.SinkInfo;
import co.cask.cdap.etl.common.TransformDetail;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformInfo;
import co.cask.cdap.etl.common.TransformResponse;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * MapReduce Driver for ETL Batch Applications.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  private static final Type SINK_OUTPUTS_TYPE = new TypeToken<List<SinkOutput>>() { }.getType();
  private static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String RUNTIME_ARGS_KEY_PREFIX = "cdap.etl.runtime.args.";


  private static final Gson GSON = new Gson();

  private static final org.apache.avro.Schema AVRO_ERROR_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(Constants.ERROR_SCHEMA.toString());

  private BatchConfigurable<BatchSourceContext> batchSource;
  private MapReduceSourceContext sourceContext;

  private Map<String, BatchConfigurable<BatchSinkContext>> batchSinks;
  private Map<String, MapReduceSinkContext> sinkContexts;

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

    Pipeline pipeline =
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

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(pipeline));
    properties.put(Constants.STAGE_LOGGING_ENABLED, String.valueOf(config.isStageLoggingEnabled()));
    properties.put(Constants.PRECONDITIONS, GSON.toJson(config.getPreconditions()));
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    if (Boolean.valueOf(context.getSpecification().getProperty(Constants.STAGE_LOGGING_ENABLED))) {
      LogStageInjector.start();
    }
    Job job = context.getHadoopJob();
    Configuration hConf = job.getConfiguration();
    Map<String, String> properties = context.getSpecification().getProperties();

    // check preconditions
    co.cask.cdap.etl.api.Preconditions preconditions = GSON.fromJson(
      properties.get(Constants.PRECONDITIONS), co.cask.cdap.etl.api.Preconditions.class);
    long timeoutSec = preconditions.getTimeoutSec();
    long timeStarted = System.currentTimeMillis();
    Queue<Condition> remainingConditions = new LinkedList<>(preconditions.getConditions());
    while (!remainingConditions.isEmpty()) {
      if (System.currentTimeMillis() - timeStarted > timeoutSec) {
        break;
      }
      Condition condition = remainingConditions.peek();
      if (condition.check(context)) {
        remainingConditions.poll();
      }
      Thread.sleep(1000);
    }
    if (!remainingConditions.isEmpty()) {
      throw new RuntimeException("Failed to pass preconditions: " + GSON.toJson(remainingConditions));
    }

    Pipeline pipeline = GSON.fromJson(properties.get(Constants.PIPELINEID), Pipeline.class);
    // following should never happen
    Preconditions.checkNotNull(pipeline, "Pipeline is null");
    Preconditions.checkNotNull(pipeline.getSinks(), "Sinks could not be found in program properties");
    // empty transform list is created during pipeline register
    Preconditions.checkNotNull(pipeline.getTransforms());
    Preconditions.checkNotNull(pipeline.getConnections(), "Connections could not be found in program properties");

    String sourcePluginId = pipeline.getSource();

    batchSource = context.newPluginInstance(sourcePluginId);
    batchSource = new LoggedBatchConfigurable<>(sourcePluginId, batchSource);
    sourceContext = new MapReduceSourceContext(context, mrMetrics, new DatasetContextLookupProvider(context),
                                               sourcePluginId, context.getRuntimeArguments());
    batchSource.prepareRun(sourceContext);

    hConf.set(RUNTIME_ARGS_KEY_PREFIX + sourcePluginId,
              GSON.toJson(sourceContext.getRuntimeArguments(), RUNTIME_ARGS_TYPE));


    List<TransformInfo> transformInfos = pipeline.getTransforms();

    // setup time partition for each error dataset
    for (TransformInfo transformInfo : transformInfos) {
      if (transformInfo.getErrorDatasetName() != null) {
        addPropertiesToErrorDataset(transformInfo.getErrorDatasetName(), context);
      }
    }

    List<SinkOutput> sinkOutputs = new ArrayList<>();

    List<SinkInfo> sinkInfos = pipeline.getSinks();
    batchSinks = new HashMap<>(sinkInfos.size());
    sinkContexts = new HashMap<>(sinkInfos.size());
    for (SinkInfo sinkInfo : sinkInfos) {
      BatchConfigurable<BatchSinkContext> batchSink = context.newPluginInstance(sinkInfo.getSinkId());
      batchSink = new LoggedBatchConfigurable<>(sinkInfo.getSinkId(), batchSink);
      MapReduceSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics,
                                                                  new DatasetContextLookupProvider(context),
                                                                  sinkInfo.getSinkId(), context.getRuntimeArguments());
      sinkContexts.put(sinkInfo.getSinkId(), sinkContext);
      batchSinks.put(sinkInfo.getSinkId(), batchSink);

      batchSink.prepareRun(sinkContext);
      sinkOutputs.add(new SinkOutput(sinkInfo.getSinkId(), sinkContext.getOutputNames(),
                                     sinkInfo.getErrorDatasetName()));

      if (sinkInfo.getErrorDatasetName() != null) {
        addPropertiesToErrorDataset(sinkInfo.getErrorDatasetName(), context);
      }
      hConf.set(RUNTIME_ARGS_KEY_PREFIX + sinkInfo.getSinkId(),
                GSON.toJson(sinkContext.getRuntimeArguments(), RUNTIME_ARGS_TYPE));
    }
    hConf.set(SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));

    job.setMapperClass(ETLMapper.class);
    job.setNumReduceTasks(0);
  }

  private void addPropertiesToErrorDataset(String errorDatasetName, MapReduceContext context) {
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "avro.schema.output.key",
             Constants.ERROR_SCHEMA.toString());
    TimePartitionedFileSetArguments.setOutputPartitionTime(args, context.getLogicalStartTime());
    context.addOutput(errorDatasetName, args);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    onRunFinishSource(succeeded);
    onRunFinishSinks(context, succeeded);
    LOG.info("Batch Run finished : succeeded = {}", succeeded);
  }


  private void onRunFinishSource(boolean succeeded) {

    LOG.info("On RunFinish Source : {}", batchSource.getClass().getName());
    try {
      batchSource.onRunFinish(succeeded, sourceContext);
    } catch (Throwable t) {
      LOG.warn("Exception when calling onRunFinish on {}", batchSource, t);
    }
  }


  private void onRunFinishSinks(MapReduceContext context, boolean succeeded) {
    String pipelineStr = context.getSpecification().getProperty(Constants.PIPELINEID);
    // should never happen
    Preconditions.checkNotNull(pipelineStr, "pipeline could not be found in program properties.");

    List<SinkInfo> sinkInfos = GSON.fromJson(pipelineStr, Pipeline.class).getSinks();
    for (SinkInfo sinkInfo : sinkInfos) {
      BatchConfigurable<BatchSinkContext> batchSink = batchSinks.get(sinkInfo.getSinkId());
      MapReduceSinkContext sinkContext = sinkContexts.get(sinkInfo.getSinkId());
      try {
        batchSink.onRunFinish(succeeded, sinkContext);
      } catch (Throwable t) {
        LOG.warn("Exception when calling onRunFinish on {}", batchSink, t);
      }
    }
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(ETLMapper.class);
    private static final Gson GSON = new Gson();
    private Set<String> transformsWithoutErrorDataset;

    private TransformExecutor<KeyValue> transformExecutor;
    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics mapperMetrics;
    private Map<String, WrappedSink<Object, Object, Object>> sinks;
    private Map<String, ErrorSink<Object, Object>> transformErrorSinkMap;

    @Override
    public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {
      // get source, transform, sink ids from program properties
      Map<String, String> properties = context.getSpecification().getProperties();
      if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
        LogStageInjector.start();
      }

      // get the list of sinks, and the names of the outputs each sink writes to
      Context hadoopContext = context.getHadoopContext();
      Configuration hConf = hadoopContext.getConfiguration();

      Pipeline pipeline = GSON.fromJson(properties.get(Constants.PIPELINEID), Pipeline.class);
      // following should never happen
      Preconditions.checkNotNull(pipeline, "Pipeline is null");
      Preconditions.checkNotNull(pipeline.getSinks(), "Sinks could not be found in program properties");
      // empty transform list is created during pipeline register
      Preconditions.checkNotNull(pipeline.getTransforms());
      Preconditions.checkNotNull(pipeline.getConnections(), "Connections could not be found in program properties");

      String sourcePluginId = pipeline.getSource();
      DefaultEmitter defaultEmitter = new DefaultEmitter(mapperMetrics);

      List<TransformInfo> transformInfos = pipeline.getTransforms();
      Map<String, List<String>> connectionsMap = pipeline.getConnections();
      Map<String, TransformDetail> transformations = new HashMap<>();

      BatchSource source = context.newPluginInstance(sourcePluginId);
      source = new LoggedBatchSource(sourcePluginId, source);
      BatchRuntimeContext runtimeContext = new MapReduceRuntimeContext(
        context, mapperMetrics, new DatasetContextLookupProvider(context), sourcePluginId,
        GSON.<Map<String, String>>fromJson(hConf.get(RUNTIME_ARGS_KEY_PREFIX + sourcePluginId), RUNTIME_ARGS_TYPE));
      source.initialize(runtimeContext);
      transformations.put(sourcePluginId,
                          new TransformDetail(source, new DefaultStageMetrics(mapperMetrics, sourcePluginId),
                                              connectionsMap.get(sourcePluginId)));

      transformErrorSinkMap = new HashMap<>();
      transformsWithoutErrorDataset = new HashSet<>();
      addTransforms(transformations, connectionsMap, transformInfos, context);

      String sinkOutputsStr = hadoopContext.getConfiguration().get(SINK_OUTPUTS_KEY);
      // should never happen, this is set in beforeSubmit
      Preconditions.checkNotNull(sinkOutputsStr, "Sink outputs not found in Hadoop conf.");

      List<SinkOutput> sinkOutputs = GSON.fromJson(sinkOutputsStr, SINK_OUTPUTS_TYPE);

      // should never happen, this is checked and set in beforeSubmit
      Preconditions.checkArgument(!sinkOutputs.isEmpty(), "Sink outputs not found in Hadoop conf.");

      boolean hasOneOutput = hasOneOutput(transformInfos, sinkOutputs);
      sinks = new HashMap<>(sinkOutputs.size());
      for (SinkOutput sinkOutput : sinkOutputs) {
        String sinkPluginId = sinkOutput.getSinkPluginId();
        Set<String> sinkOutputNames = sinkOutput.getSinkOutputs();

        BatchSink<Object, Object, Object> sink = context.newPluginInstance(sinkPluginId);
        sink = new LoggedBatchSink<>(sinkPluginId, sink);
        runtimeContext = new MapReduceRuntimeContext(
          context, mapperMetrics, new DatasetContextLookupProvider(context), sinkPluginId,
          GSON.<Map<String, String>>fromJson(hConf.get(RUNTIME_ARGS_KEY_PREFIX + sinkPluginId), RUNTIME_ARGS_TYPE));
        sink.initialize(runtimeContext);
        if (hasOneOutput) {
          sinks.put(sinkPluginId, new SingleOutputSink<>(sink, context));
        } else {
          sinks.put(sinkPluginId, new MultiOutputSink<>(sink, context, sinkOutputNames));
        }
        transformations.put(sinkPluginId,
                            new TransformDetail(sink, new DefaultStageMetrics(mapperMetrics, sinkPluginId),
                                                new ArrayList<String>()));
      }

      transformExecutor = new TransformExecutor<>(transformations, ImmutableList.of(sourcePluginId));

    }


    // this is needed because we need to write to the context differently depending on the number of outputs
    private boolean hasOneOutput(List<TransformInfo> transformInfos, List<SinkOutput> sinkOutputs) {
      // if there are any error datasets, we know we have at least one sink, and one error dataset
      for (TransformInfo info : transformInfos) {
        if (info.getErrorDatasetName() != null) {
          return false;
        }
      }
      // if no error datasets, check if we have more than one sink
      Set<String> allOutputs = new HashSet<>();

      for (SinkOutput sinkOutput : sinkOutputs) {
        if (sinkOutput.getErrorDatasetName() != null) {
          return false;
        }
        allOutputs.addAll(sinkOutput.getSinkOutputs());
      }
      return allOutputs.size() == 1;
    }

    private void addTransforms(Map<String, TransformDetail> pipeline,
                               Map<String, List<String>> connectionsMap,
                               List<TransformInfo> transformInfos,
                               MapReduceTaskContext context) throws Exception {

      for (TransformInfo transformInfo : transformInfos) {
        String transformId = transformInfo.getTransformId();
        Transform<?, ?> transform = context.newPluginInstance(transformId);
        transform = new LoggedTransform<>(transformId, transform);
        BatchRuntimeContext transformContext = new MapReduceRuntimeContext(
          context, mapperMetrics, new DatasetContextLookupProvider(context), transformId,
          context.getRuntimeArguments());
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        pipeline.put(transformId,
                     new TransformDetail(transform, new DefaultStageMetrics(mapperMetrics, transformId),
                                         connectionsMap.get(transformId)));
        if (transformInfo.getErrorDatasetName() != null) {
          transformErrorSinkMap.put(transformId,
                                    new ErrorSink<>(context, transformInfo.getErrorDatasetName()));
        }
      }
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        KeyValue<Object, Object> input = new KeyValue<>(key, value);
        TransformResponse transformResponse = transformExecutor.runOneIteration(input);
        for (Map.Entry<String, Collection<Object>> transformedEntry : transformResponse.getSinksResults().entrySet()) {
          WrappedSink<Object, Object, Object> sink = sinks.get(transformedEntry.getKey());
          for (Object transformedRecord : transformedEntry.getValue()) {
            sink.write((KeyValue<Object, Object>) transformedRecord);
          }
        }

        for (Map.Entry<String, Collection<InvalidEntry<Object>>> errorEntries :
          transformResponse.getMapTransformIdToErrorEmitter().entrySet()) {

          if (transformsWithoutErrorDataset.contains(errorEntries.getKey())) {
            continue;
          }
          if (!errorEntries.getValue().isEmpty()) {
            if (!transformErrorSinkMap.containsKey(errorEntries.getKey())) {
              LOG.warn("Transform : {} has error records, but does not have a error dataset configured.",
                       errorEntries.getKey());
              transformsWithoutErrorDataset.add(errorEntries.getKey());
            } else {
              transformErrorSinkMap.get(errorEntries.getKey()).write(errorEntries.getValue());
            }
          }
        }
        transformExecutor.resetEmitter();
      } catch (Exception e) {
        LOG.error("Exception thrown in BatchDriver Mapper.", e);
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      // BatchSource implements Transform, hence is inside the transformExecutor as well
      Destroyables.destroyQuietly(transformExecutor);
      // Cleanup BatchSinks separately, since they are not part of the transformExecutor
      LOG.debug("Number of sinks to destroy: {}", sinks.size());
      for (WrappedSink<Object, Object, Object> sink : sinks.values()) {
        LOG.trace("Destroying sink: {}", sink.sink);
        Destroyables.destroyQuietly(sink.sink);
      }
    }
  }

  private static class ErrorSink<KEY_OUT, VAL_OUT> {
    private final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;
    private final String errorDatasetName;

    private ErrorSink(MapReduceTaskContext<KEY_OUT, VAL_OUT> context, String errorDatasetName) {
      this.context = context;
      this.errorDatasetName = errorDatasetName;
    }

    public void write(Collection<InvalidEntry<Object>> input) throws Exception {
      for (InvalidEntry entry : input) {
        context.write(errorDatasetName, new AvroKey<>(getGenericRecordForInvalidEntry(entry)),
                      NullWritable.get());
      }
    }
  }

  // wrapper around sinks to help writing sink output to the correct named output
  private abstract static class WrappedSink<IN, KEY_OUT, VAL_OUT> {
    protected final BatchSink<IN, KEY_OUT, VAL_OUT> sink;
    protected final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;

    protected WrappedSink(BatchSink<IN, KEY_OUT, VAL_OUT> sink,
                          MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
      this.sink = sink;
      this.context = context;
    }

    protected abstract void write(KeyValue<KEY_OUT, VAL_OUT> output) throws Exception;
  }

  // need to write with a different method if there is only one output for the mapreduce
  // TODO: remove if the fix to CDAP-3628 allows us to write using the same method
  private static class SingleOutputSink<IN, KEY_OUT, VAL_OUT> extends WrappedSink<IN, KEY_OUT, VAL_OUT> {

    protected SingleOutputSink(BatchSink<IN, KEY_OUT, VAL_OUT> sink,
                               MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
      super(sink, context);
    }

    public void write(KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
      context.write(output.getKey(), output.getValue());
    }
  }

  // writes sink output to the correct named output
  private static class MultiOutputSink<IN, KEY_OUT, VAL_OUT> extends WrappedSink<IN, KEY_OUT, VAL_OUT> {
    private final Set<String> outputNames;

    private MultiOutputSink(BatchSink<IN, KEY_OUT, VAL_OUT> sink,
                            MapReduceTaskContext<KEY_OUT, VAL_OUT> context,
                            Set<String> outputNames) {
      super(sink, context);
      this.outputNames = outputNames;
    }

    public void write(KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
      for (String outputName : outputNames) {
        context.write(outputName, output.getKey(), output.getValue());
      }
    }
  }

  private static GenericRecord getGenericRecordForInvalidEntry(InvalidEntry invalidEntry) {
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AVRO_ERROR_SCHEMA);
    recordBuilder.set(Constants.ErrorDataset.ERRCODE, invalidEntry.getErrorCode());
    recordBuilder.set(Constants.ErrorDataset.ERRMSG, invalidEntry.getErrorMsg());

    String errorMsg;
    if (invalidEntry.getInvalidRecord() instanceof StructuredRecord) {
      StructuredRecord record = (StructuredRecord) invalidEntry.getInvalidRecord();
      try {
        errorMsg = StructuredRecordStringConverter.toJsonString(record);
      } catch (IOException e) {
        errorMsg = "Exception while converting StructuredRecord to String, " + e.getCause();
      }
    } else {
      errorMsg = String.format("Error Entry is of type %s, only a record of type %s is supported currently",
                               invalidEntry.getInvalidRecord().getClass().getName(),
                               StructuredRecord.class.getName());
    }
    recordBuilder.set(Constants.ErrorDataset.INVALIDENTRY, errorMsg);
    return recordBuilder.build();
  }

  private static class SinkOutput {
    private String sinkPluginId;
    private Set<String> sinkOutputs;
    private String errorDatasetName;

    private SinkOutput(String sinkPluginId, Set<String> sinkOutputs, String errorDatasetName) {
      this.sinkPluginId = sinkPluginId;
      this.sinkOutputs = sinkOutputs;
      this.errorDatasetName = errorDatasetName;
    }

    public String getSinkPluginId() {
      return sinkPluginId;
    }

    public Set<String> getSinkOutputs() {
      return sinkOutputs;
    }

    public String getErrorDatasetName() {
      return errorDatasetName;
    }

  }
}
