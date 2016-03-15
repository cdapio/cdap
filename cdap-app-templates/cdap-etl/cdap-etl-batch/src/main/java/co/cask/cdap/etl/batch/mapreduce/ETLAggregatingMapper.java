package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.batch.BatchAggregation;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformResponse;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
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
 * Mapper Driver for ETL Transforms with an aggregator.
 */
public class ETLAggregatingMapper extends Mapper implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {

  public static final String RUNTIME_ARGS_KEY = "cdap.etl.runtime.args";
  public static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  private static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, Map<String, String>>>() { }.getType();
  private static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, SinkOutput>>() { }.getType();
  private static final Type AGGREGATOR_CONFIG_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(ETLAggregatingMapper.class);
  private static final Gson GSON = new Gson();

  private Set<String> transformsWithoutErrorDataset;

  private TransformExecutor<KeyValue<Object, Object>> transformExecutor;
  // injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;
  private Map<String, ErrorSink<Object, Object>> transformErrorSinkMap;
  private String groupByKey;

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

    PipelinePhase pipeline = GSON.fromJson(properties.get(Constants.PIPELINEID), PipelinePhase.class);
    Map<String, Map<String, String>> runtimeArgs = GSON.fromJson(hConf.get(RUNTIME_ARGS_KEY), RUNTIME_ARGS_TYPE);
    MapReduceTransformExecutorFactory<KeyValue<Object, Object>> transformExecutorFactory =
      new MapReduceTransformExecutorFactory<>(context, metrics, context.getLogicalStartTime(), runtimeArgs);
    transformExecutor = transformExecutorFactory.create(pipeline);

    transformErrorSinkMap = new HashMap<>();
    for (StageInfo transformInfo : pipeline.getTransforms()) {
      String errorDatasetName = transformInfo.getErrorDatasetName();
      if (errorDatasetName != null) {
        transformErrorSinkMap.put(transformInfo.getName(), new ErrorSink<>(context, errorDatasetName));
      }
    }

    String sinkOutputsStr = hadoopContext.getConfiguration().get(SINK_OUTPUTS_KEY);
    // should never happen, this is set in beforeSubmit
    Preconditions.checkNotNull(sinkOutputsStr, "Sink outputs not found in Hadoop conf.");
    Map<String, SinkOutput> sinkOutputs = GSON.fromJson(sinkOutputsStr, SINK_OUTPUTS_TYPE);

    Map<String, String> aggregatorConfig =
      GSON.fromJson(hConf.get(Constants.AGGREGATOR_CONFIG), AGGREGATOR_CONFIG_TYPE);
    groupByKey = aggregatorConfig.get(BatchAggregation.PROP_GROUP_BY);
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
          // TODO: multiple sinks
          Preconditions.checkArgument(transformedRecord instanceof StructuredRecord);
          StructuredRecord sRecord = (StructuredRecord) transformedRecord;
          Preconditions.checkArgument(sRecord.get(groupByKey) != null);
          context.write(sRecord.get(groupByKey), transformedRecord);
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
