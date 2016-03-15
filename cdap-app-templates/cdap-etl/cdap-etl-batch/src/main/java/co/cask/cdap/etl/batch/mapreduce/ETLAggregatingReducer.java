/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.batch.BatchAggregation;
import co.cask.cdap.etl.batch.FunctionConfig;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reducer Driver for ETL Transforms with an aggregator.
 */
public class ETLAggregatingReducer extends Reducer implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {

  public static final String RUNTIME_ARGS_KEY = "cdap.etl.runtime.args";
  public static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  private static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, Map<String, String>>>() { }.getType();
  private static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, SinkOutput>>() { }.getType();
  private static final Type AGGREGATOR_CONFIG_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type FUNCTIONS_TYPE = new TypeToken<List<FunctionConfig>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapper.class);
  private static final Gson GSON = new Gson();

  // injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;

  private Set<String> transformsWithoutErrorDataset;
  private TransformExecutor<KeyValue<Object, Object>> transformExecutor;
  private SinkWriter<Object, Object> sinkWriter;
  private Map<String, ErrorSink<Object, Object>> transformErrorSinkMap;
  private Map<String, FunctionConfig> functions;

  @Override
  public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {
    // get source, transform, sink ids from program properties
    Map<String, String> properties = context.getSpecification().getProperties();
    if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
      LogStageInjector.start();
    }

    // get the list of sinks, and the names of the outputs each sink writes to
    Mapper.Context hadoopContext = context.getHadoopContext();
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

    sinkWriter = hasOneOutput(pipeline.getTransforms(), sinkOutputs) ?
      new SingleOutputWriter<>(context) : new MultiOutputWriter<>(context, sinkOutputs);

    Map<String, String> aggregatorConfig =
      GSON.fromJson(hConf.get(Constants.AGGREGATOR_CONFIG), AGGREGATOR_CONFIG_TYPE);
    functions = GSON.fromJson(aggregatorConfig.get(BatchAggregation.PROP_FUNCTIONS), FUNCTIONS_TYPE);
  }

  @Override
  protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
    int x = 0;
  }

  @Override
  public void destroy() {
    // BatchSource and BatchSink both implement Transform, hence are inside the transformExecutor as well
    Destroyables.destroyQuietly(transformExecutor);
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

}
