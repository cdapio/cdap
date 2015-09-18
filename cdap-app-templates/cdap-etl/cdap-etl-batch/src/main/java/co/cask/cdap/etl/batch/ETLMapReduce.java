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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.Pipeline;
import co.cask.cdap.etl.common.PipelineRegisterer;
import co.cask.cdap.etl.common.PluginID;
import co.cask.cdap.etl.common.SinkInfo;
import co.cask.cdap.etl.common.StageMetrics;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import co.cask.cdap.etl.common.TransformDetail;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformInfo;
import co.cask.cdap.etl.common.TransformResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * MapReduce driver for Batch ETL Adapters.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  private static final Type SINK_OUTPUTS_TYPE = new TypeToken<List<SinkOutput>>() { }.getType();
  private static final Type SINK_INFO_TYPE = new TypeToken<List<SinkInfo>>() { }.getType();
  private static final Type TRANSFORMINFO_LIST_TYPE = new TypeToken<List<TransformInfo>>() { }.getType();

  private static final Gson GSON = new Gson();

  @VisibleForTesting
  static final Schema ERROR_SCHEMA = Schema.recordOf(
    "error",
    Schema.Field.of(Constants.ErrorDataset.ERRCODE, Schema.of(Schema.Type.INT)),
    Schema.Field.of(Constants.ErrorDataset.ERRMSG, Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                                  Schema.of(Schema.Type.NULL))),
    Schema.Field.of(Constants.ErrorDataset.INVALIDENTRY, Schema.of(Schema.Type.STRING)));

  private static final org.apache.avro.Schema AVRO_ERROR_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(ERROR_SCHEMA.toString());

  private BatchConfigurable<BatchSourceContext> batchSource;
  private List<BatchConfigurable<BatchSinkContext>> batchSinks;
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
    setDescription("MapReduce driver for Batch ETL Adapters");

    PipelineRegisterer pipelineRegisterer = new PipelineRegisterer(getConfigurer(), "batch");

    Pipeline pipelineIds =
      pipelineRegisterer.registerPlugins(
        config, TimePartitionedFileSet.class,
        FileSetProperties.builder()
          .setInputFormat(AvroKeyInputFormat.class)
          .setOutputFormat(AvroKeyOutputFormat.class)
          .setEnableExploreOnCreate(true)
          .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
          .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
          .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
          .setTableProperty("avro.schema.literal", ERROR_SCHEMA.toString())
          .build(), true);

    if (config.getResources() != null) {
      setMapperResources(config.getResources());
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Source.PLUGINID, pipelineIds.getSource());
    properties.put(Constants.Sink.PLUGINIDS, GSON.toJson(pipelineIds.getSinks()));
    properties.put(Constants.Transform.PLUGINIDS, GSON.toJson(pipelineIds.getTransforms()));
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();

    Map<String, String> properties = context.getSpecification().getProperties();
    String sourcePluginId = properties.get(Constants.Source.PLUGINID);

    batchSource = context.newPluginInstance(sourcePluginId);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    batchSource.prepareRun(sourceContext);

    String transformInfosStr = properties.get(Constants.Transform.PLUGINIDS);
    Preconditions.checkNotNull(transformInfosStr, "Transform plugin ids not found in program properties.");

    List<TransformInfo> transformInfos = GSON.fromJson(transformInfosStr, TRANSFORMINFO_LIST_TYPE);

    // setup time partition for each error dataset
    for (TransformInfo transformInfo : transformInfos) {
      if (transformInfo.getErrorDatasetName() != null) {
        addPropertiesToErrorDataset(transformInfo.getErrorDatasetName(), context);
      }
    }

    List<SinkOutput> sinkOutputs = new ArrayList<>();
    String sinkPluginIdsStr = properties.get(Constants.Sink.PLUGINIDS);
    // should never happen
    Preconditions.checkNotNull(sinkPluginIdsStr, "sink plugin ids could not be found in program properties.");

    List<SinkInfo> sinkInfos = GSON.fromJson(sinkPluginIdsStr, SINK_INFO_TYPE);
    batchSinks = Lists.newArrayListWithCapacity(sinkInfos.size());
    for (SinkInfo sinkInfo : sinkInfos) {
      BatchConfigurable<BatchSinkContext> batchSink = context.newPluginInstance(sinkInfo.getSinkId());
      MapReduceSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkInfo.getSinkId());
      batchSink.prepareRun(sinkContext);
      batchSinks.add(batchSink);
      sinkOutputs.add(new SinkOutput(sinkInfo.getSinkId(), sinkContext.getOutputNames(),
                                     sinkInfo.getErrorDatasetName()));

      if (sinkInfo.getErrorDatasetName() != null) {
        addPropertiesToErrorDataset(sinkInfo.getErrorDatasetName(), context);
      }

    }
    job.getConfiguration().set(SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));

    job.setMapperClass(ETLMapper.class);
    job.setNumReduceTasks(0);
  }

  private void addPropertiesToErrorDataset(String errorDatasetName, MapReduceContext context) {
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "avro.schema.output.key", ERROR_SCHEMA.toString());
    TimePartitionedFileSetArguments.setOutputPartitionTime(args, context.getLogicalStartTime());
    context.addOutput(errorDatasetName, args);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    onRunFinishSource(context, succeeded);
    onRunFinishSink(context, succeeded);
    LOG.info("Batch Run finished : succeeded = {}", succeeded);
  }

  private void onRunFinishSource(MapReduceContext context, boolean succeeded) {
    String sourcePluginId = context.getSpecification().getProperty(Constants.Source.PLUGINID);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    LOG.info("On RunFinish Source : {}", batchSource.getClass().getName());
    try {
      batchSource.onRunFinish(succeeded, sourceContext);
    } catch (Throwable t) {
      LOG.warn("Exception when calling onRunFinish on {}", batchSource, t);
    }
  }

  private void onRunFinishSink(MapReduceContext context, boolean succeeded) {
    String sinkPluginIdsStr = context.getSpecification().getProperty(Constants.Sink.PLUGINIDS);
    // should never happen
    Preconditions.checkNotNull(sinkPluginIdsStr, "sink plugin ids could not be found in program properties.");

    List<SinkInfo> sinkInfos = GSON.fromJson(sinkPluginIdsStr, SINK_INFO_TYPE);
    for (int i = 0; i < sinkInfos.size(); i++) {
      BatchConfigurable<BatchSinkContext> batchSink = batchSinks.get(i);
      String sinkPluginId = sinkInfos.get(i).getSinkId();
      BatchSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
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
    private static final Type TRANSFORMDETAILS_LIST_TYPE = new TypeToken<List<TransformInfo>>() { }.getType();
    private Set<String> transformsWithoutErrorDataset;

    private TransformExecutor<KeyValue, Object> transformExecutor;
    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics mapperMetrics;
    private List<WrappedSink<Object, Object, Object>> sinks;
    private Map<String, ErrorSink<Object, Object, Object>> transformErrorSinkMap;

    @Override
    public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {
      // get source, transform, sink ids from program properties
      context.getSpecification().getProperties();
      Map<String, String> properties = context.getSpecification().getProperties();

      String sourcePluginId = properties.get(Constants.Source.PLUGINID);
      // should never happen
      String transformInfosStr = properties.get(Constants.Transform.PLUGINIDS);
      Preconditions.checkNotNull(transformInfosStr, "Transform plugin ids not found in program properties.");

      List<TransformInfo> transformInfos = GSON.fromJson(transformInfosStr, TRANSFORMDETAILS_LIST_TYPE);
      List<TransformDetail> pipeline = Lists.newArrayListWithCapacity(transformInfos.size() + 2);

      BatchSource source = context.newPluginInstance(sourcePluginId);
      BatchRuntimeContext runtimeContext = new MapReduceRuntimeContext(context, mapperMetrics, sourcePluginId);
      source.initialize(runtimeContext);
      pipeline.add(new TransformDetail(sourcePluginId, source,
        new StageMetrics(mapperMetrics, PluginID.from(sourcePluginId))));

      transformErrorSinkMap = new HashMap<>();
      transformsWithoutErrorDataset = new HashSet<>();
      addTransforms(pipeline, transformInfos, context);

      // get the list of sinks, and the names of the outputs each sink writes to
      Context hadoopContext = context.getHadoopContext();
      String sinkOutputsStr = hadoopContext.getConfiguration().get(SINK_OUTPUTS_KEY);
      // should never happen, this is set in beforeSubmit
      Preconditions.checkNotNull(sinkOutputsStr, "Sink outputs not found in hadoop conf.");

      List<SinkOutput> sinkOutputs = GSON.fromJson(sinkOutputsStr, SINK_OUTPUTS_TYPE);

      // should never happen, this is checked and set in beforeSubmit
      Preconditions.checkArgument(!sinkOutputs.isEmpty(), "Sink outputs not found in hadoop conf.");

      boolean hasOneOutput = hasOneOutput(transformInfos, sinkOutputs);
      sinks = new ArrayList<>(sinkOutputs.size());
      for (SinkOutput sinkOutput : sinkOutputs) {
        String sinkPluginId = sinkOutput.getSinkPluginId();
        Set<String> sinkOutputNames = sinkOutput.getSinkOutputs();

        BatchSink<Object, Object, Object> sink = context.newPluginInstance(sinkPluginId);
        runtimeContext = new MapReduceRuntimeContext(context, mapperMetrics, sinkPluginId);
        sink.initialize(runtimeContext);
        if (hasOneOutput) {
          sinks.add(new SingleOutputSink<>(sinkPluginId, sink, context, mapperMetrics));
        } else {
          sinks.add(new MultiOutputSink<>(sinkPluginId, sink, context, mapperMetrics, sinkOutputNames,
                                          sinkOutput.getErrorDatasetName()));
        }
      }

      transformExecutor = new TransformExecutor<>(pipeline);
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

    private void addTransforms(List<TransformDetail> pipeline,
                               List<TransformInfo> transformInfos,
                               MapReduceTaskContext context) throws Exception {

      for (TransformInfo transformInfo : transformInfos) {
        String transformId = transformInfo.getTransformId();
        Transform transform = context.newPluginInstance(transformId);
        BatchRuntimeContext transformContext = new MapReduceRuntimeContext(context, mapperMetrics, transformId);
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        pipeline.add(new TransformDetail(transformId, transform,
                                               new StageMetrics(mapperMetrics, PluginID.from(transformId))));
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
        TransformResponse<Object> recordsAndErrors = transformExecutor.runOneIteration(input);
        Iterator<Object> transformedRecords = recordsAndErrors.getEmittedRecords();
        while (transformedRecords.hasNext()) {
          Object transformedRecord = transformedRecords.next();
          for (WrappedSink<Object, Object, Object> sink : sinks) {
            sink.write(transformedRecord);
          }
        }
        // write the error entries to transform error datasets.
        for (Map.Entry<String, Collection<Object>> entry :
          recordsAndErrors.getMapTransformIdToErrorEmitter().entrySet()) {
          if (transformErrorSinkMap.containsKey(entry.getKey())) {
            transformErrorSinkMap.get(entry.getKey()).write(entry.getValue());
          } else {
            if (!transformsWithoutErrorDataset.contains(entry.getKey())) {
              LOG.warn("Transform : {} has error records, but does not have a error dataset configured.",
                       entry.getKey());
              transformsWithoutErrorDataset.add(entry.getKey());
            }
          }
        }

        transformExecutor.resetEmitters();
      } catch (Exception e) {
        LOG.error("Exception thrown in BatchDriver Mapper : {}", e);
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      // Both BatchSource and BatchSink implements Transform, hence are inside the transformExecutor as well
      Destroyables.destroyQuietly(transformExecutor);
    }
  }

  private static class ErrorSink<IN, KEY_OUT, VAL_OUT> {
    private final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;
    private final String errorDatasetName;

    private ErrorSink(MapReduceTaskContext<KEY_OUT, VAL_OUT> context, String errorDatasetName) {
      this.context = context;
      this.errorDatasetName = errorDatasetName;
    }

    public void write(Collection<IN> input) throws Exception {
      for (IN entry : input) {
        context.write(errorDatasetName, new AvroKey<>(getGenericRecordForInvalidEntry((InvalidEntry) entry)),
                      NullWritable.get());
      }
    }
  }

  // wrapper around sinks to help writing sink output to the correct named output
  private abstract static class WrappedSink<IN, KEY_OUT, VAL_OUT> {
    protected final BatchSink<IN, KEY_OUT, VAL_OUT> sink;
    protected final DefaultEmitter<KeyValue<KEY_OUT, VAL_OUT>> emitter;
    protected final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;

    protected WrappedSink(String sinkPluginId,
                          BatchSink<IN, KEY_OUT, VAL_OUT> sink,
                          MapReduceTaskContext<KEY_OUT, VAL_OUT> context,
                          Metrics metrics) {
      this.sink = sink;
      this.emitter = new DefaultEmitter<>(new StageMetrics(metrics, PluginID.from(sinkPluginId)));
      this.context = context;
    }

    protected abstract void write(IN input) throws Exception;
  }

  // need to write with a different method if there is only one output for the mapreduce
  // TODO: remove if the fix to CDAP-3628 allows us to write using the same method
  private static class SingleOutputSink<IN, KEY_OUT, VAL_OUT> extends WrappedSink<IN, KEY_OUT, VAL_OUT> {

    protected SingleOutputSink(String sinkPluginId, BatchSink<IN, KEY_OUT, VAL_OUT> sink,
                               MapReduceTaskContext<KEY_OUT, VAL_OUT> context, Metrics metrics) {
      super(sinkPluginId, sink, context, metrics);
    }

    public void write(IN input) throws Exception {
      sink.transform(input, emitter);
      for (KeyValue<KEY_OUT, VAL_OUT> outputRecord : emitter) {
        context.write(outputRecord.getKey(), outputRecord.getValue());
      }
      emitter.reset();
    }
  }

  // writes sink output to the correct named output
  private static class MultiOutputSink<IN, KEY_OUT, VAL_OUT> extends WrappedSink<IN, KEY_OUT, VAL_OUT> {
    private final Set<String> outputNames;
    private final String errorDatasetName;

    private MultiOutputSink(String sinkPluginId,
                            BatchSink<IN, KEY_OUT, VAL_OUT> sink,
                            MapReduceTaskContext<KEY_OUT, VAL_OUT> context,
                            Metrics metrics,
                            Set<String> outputNames,
                            @Nullable String errorDatasetName) {
      super(sinkPluginId, sink, context, metrics);
      this.outputNames = outputNames;
      this.errorDatasetName = errorDatasetName;
    }

    public void write(IN input) throws Exception {
      sink.transform(input, emitter);
      for (KeyValue outputRecord : emitter) {
        for (String outputName : outputNames) {
          context.write(outputName, outputRecord.getKey(), outputRecord.getValue());
        }
      }

      if (errorDatasetName != null && !emitter.getErrors().isEmpty()) {
        for (InvalidEntry entry : emitter.getErrors()) {
          context.write(errorDatasetName, new AvroKey<>(getGenericRecordForInvalidEntry(entry)), NullWritable.get());
        }
      }

      emitter.reset();
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
      errorMsg = String.format("Error Entry is of type %s, only records of type %s is supported currently",
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
