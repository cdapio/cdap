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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.CompositeFinisher;
import co.cask.cdap.etl.batch.Finisher;
import co.cask.cdap.etl.batch.LoggedBatchConfigurable;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformResponse;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    if (phaseSpec.getPhase().getSources().size() != 1) {
      throw new IllegalArgumentException("Pipeline must contain exactly one source.");
    }
    if (phaseSpec.getPhase().getSinks().isEmpty()) {
      throw new IllegalArgumentException("Pipeline must contain at least one sink.");
    }

    setMapperResources(phaseSpec.getResources());
    setDriverResources(phaseSpec.getResources());
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
    hConf.set(ETLMapper.SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));

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
    job.setNumReduceTasks(0);

    hConf.set(ETLMapper.RUNTIME_ARGS_KEY, GSON.toJson(runtimeArgs));
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
    public static final String RUNTIME_ARGS_KEY = "cdap.etl.runtime.args";
    public static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
    private static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, Map<String, String>>>() { }.getType();
    private static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, SinkOutput>>() { }.getType();
    private static final Set<String> PLUGIN_TYPES = ImmutableSet.of(
      BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE, Constants.CONNECTOR_TYPE);
    private static final Logger LOG = LoggerFactory.getLogger(ETLMapper.class);
    private static final Gson GSON = new Gson();

    private Set<String> transformsWithoutErrorDataset;

    private TransformExecutor<KeyValue<Object, Object>> transformExecutor;
    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics mapperMetrics;
    private SinkWriter<Object, Object> sinkWriter;
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
      transformsWithoutErrorDataset = new HashSet<>();

      BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
      Map<String, Map<String, String>> runtimeArgs = GSON.fromJson(hConf.get(RUNTIME_ARGS_KEY), RUNTIME_ARGS_TYPE);
      PipelinePluginInstantiator pluginInstantiator = new PipelinePluginInstantiator(context, phaseSpec);

      MapReduceTransformExecutorFactory<KeyValue<Object, Object>> transformExecutorFactory =
        new MapReduceTransformExecutorFactory<>(context, pluginInstantiator, mapperMetrics, runtimeArgs);
      transformExecutor = transformExecutorFactory.create(phaseSpec.getPhase(), PLUGIN_TYPES);

      transformErrorSinkMap = new HashMap<>();
      for (StageInfo transformInfo : phaseSpec.getPhase().getStagesOfType(Transform.PLUGIN_TYPE)) {
        String errorDatasetName = transformInfo.getErrorDatasetName();
        if (errorDatasetName != null) {
          transformErrorSinkMap.put(transformInfo.getName(), new ErrorSink<>(context, errorDatasetName));
        }
      }

      String sinkOutputsStr = hadoopContext.getConfiguration().get(SINK_OUTPUTS_KEY);
      // should never happen, this is set in beforeSubmit
      Preconditions.checkNotNull(sinkOutputsStr, "Sink outputs not found in Hadoop conf.");
      Map<String, SinkOutput> sinkOutputs = GSON.fromJson(sinkOutputsStr, SINK_OUTPUTS_TYPE);

      sinkWriter = hasOneOutput(phaseSpec.getPhase().getStagesOfType(Transform.PLUGIN_TYPE), sinkOutputs) ?
        new SingleOutputWriter<>(context) : new MultiOutputWriter<>(context, sinkOutputs);
    }

    // this is needed because we need to write to the context differently depending on the number of outputs
    private boolean hasOneOutput(Set<StageInfo> transformInfos, Map<String, SinkOutput> sinkOutputs) {
      // if there are any error datasets, we know we have at least one sink, and one error dataset
      for (StageInfo info : transformInfos) {
        if (info.getErrorDatasetName() != null) {
          return false;
        }
      }
      // if no error datasets, check if we have more than one sink
      Set<String> allOutputs = new HashSet<>();

      for (SinkOutput sinkOutput : sinkOutputs.values()) {
        if (sinkOutput.getErrorDatasetName() != null) {
          return false;
        }
        allOutputs.addAll(sinkOutput.getSinkOutputs());
      }
      return allOutputs.size() == 1;
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        KeyValue<Object, Object> input = new KeyValue<>(key, value);
        TransformResponse transformResponse = transformExecutor.runOneIteration(input);
        for (Map.Entry<String, Collection<Object>> transformedEntry : transformResponse.getSinksResults().entrySet()) {
          for (Object transformedRecord : transformedEntry.getValue()) {
            sinkWriter.write(transformedEntry.getKey(), (KeyValue<Object, Object>) transformedRecord);
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
      // BatchSource and BatchSink both implement Transform, hence are inside the transformExecutor as well
      Destroyables.destroyQuietly(transformExecutor);
    }

  }

  private static class ErrorSink<KEY_OUT, VAL_OUT> {
    private static final org.apache.avro.Schema AVRO_ERROR_SCHEMA =
      new org.apache.avro.Schema.Parser().parse(Constants.ERROR_SCHEMA.toString());
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

    private GenericRecord getGenericRecordForInvalidEntry(InvalidEntry invalidEntry) {
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
  }

  // wrapper around sinks to help writing sink output to the correct named output
  // would not be needed if you could always do context.write(name, key, value)
  // but if there is just one output, you must do context.write(key, value) instead of context.write(name, key, value)
  // see CDAP-3628 for more detail
  private abstract static class SinkWriter<KEY_OUT, VAL_OUT> {
    protected final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;

    public SinkWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
      this.context = context;
    }

    protected abstract void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception;
  }

  // needed because we have to do context.write(key, value) if there is only one output.
  // see CDAP-3628 for more detail
  private static class SingleOutputWriter<KEY_OUT, VAL_OUT> extends SinkWriter<KEY_OUT, VAL_OUT> {

    protected SingleOutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
      super(context);
    }

    public void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
      context.write(output.getKey(), output.getValue());
    }
  }

  // writes sink output to the correct named output
  private static class MultiOutputWriter<KEY_OUT, VAL_OUT> extends SinkWriter<KEY_OUT, VAL_OUT> {
    // sink name -> outputs for that sink
    private final Map<String, SinkOutput> sinkOutputs;

    public MultiOutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context, Map<String, SinkOutput> sinkOutputs) {
      super(context);
      this.sinkOutputs = sinkOutputs;
    }

    public void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
      for (String outputName : sinkOutputs.get(sinkName).getSinkOutputs()) {
        context.write(outputName, output.getKey(), output.getValue());
      }
    }
  }
}
