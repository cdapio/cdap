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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformResponse;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Initializes a TransformExecutor and runs transforms. This is used in both the mapper and reducer since they
 * do mostly the same thing, except the mapper needs to write to an aggregator or to sinks, whereas the reducer
 * needs to read from an aggregator and write to sinks.
 *
 * @param <KEY> the type of key to send into the transform executor
 * @param <VALUE> the type of value to send into the transform executor
 */
public abstract class TransformRunner<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(TransformRunner.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>()).create();
  private final Set<String> transformsWithoutErrorDataset;
  private final Map<String, ErrorSink<Object, Object>> transformErrorSinkMap;
  private final TransformExecutor<KeyValue<KEY, VALUE>> transformExecutor;
  private final OutputWriter<Object, Object> outputWriter;

  public TransformRunner(MapReduceTaskContext<Object, Object> context,
                         Configuration hConf,
                         Metrics metrics) throws Exception {
    // figure out whether we are writing to a single output or to multiple outputs
    Map<String, String> properties = context.getSpecification().getProperties();
    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);
    this.outputWriter = getSinkWriter(context, phaseSpec.getPhase(), hConf);

    // instantiate and initialize all transformations and setup the TransformExecutor
    PipelinePluginInstantiator pluginInstantiator = new PipelinePluginInstantiator(context, phaseSpec);
    Map<String, Map<String, String>> runtimeArgs = GSON.fromJson(
      hConf.get(ETLMapReduce.RUNTIME_ARGS_KEY), ETLMapReduce.RUNTIME_ARGS_TYPE);
    this.transformExecutor = getTransformExecutor(context, pluginInstantiator, metrics,
                                                  runtimeArgs, phaseSpec.getPhase());

    // setup error dataset information
    this.transformsWithoutErrorDataset = new HashSet<>();
    this.transformErrorSinkMap = new HashMap<>();
    for (StageInfo transformInfo : phaseSpec.getPhase().getStagesOfType(Transform.PLUGIN_TYPE)) {
      String errorDatasetName = transformInfo.getErrorDatasetName();
      if (errorDatasetName != null) {
        transformErrorSinkMap.put(transformInfo.getName(), new ErrorSink<>(context, errorDatasetName));
      }
    }
  }

  protected abstract TransformExecutor<KeyValue<KEY, VALUE>> getTransformExecutor(
    MapReduceTaskContext<Object, Object> context,
    PipelinePluginInstantiator pluginInstantiator,
    Metrics metrics,
    Map<String, Map<String, String>> runtimeArgs,
    PipelinePhase pipelinePhase
  ) throws Exception;

  // this is needed because we need to write to the context differently depending on the number of outputs
  private OutputWriter<Object, Object> getSinkWriter(MapReduceTaskContext<Object, Object> context,
                                                     PipelinePhase pipelinePhase,
                                                     Configuration hConf) {
    Set<StageInfo> aggregators = pipelinePhase.getStagesOfType(BatchAggregator.PLUGIN_TYPE);
    if (!aggregators.isEmpty()) {
      String aggregatorName = aggregators.iterator().next().getName();
      if (pipelinePhase.getSinks().contains(aggregatorName)) {
        return new SingleOutputWriter<>(context);
      }
    }

    String sinkOutputsStr = hConf.get(ETLMapReduce.SINK_OUTPUTS_KEY);

    // should never happen, this is set in beforeSubmit
    Preconditions.checkNotNull(sinkOutputsStr, "Sink outputs not found in Hadoop conf.");
    Map<String, SinkOutput> sinkOutputs = GSON.fromJson(sinkOutputsStr, ETLMapReduce.SINK_OUTPUTS_TYPE);
    return hasSingleOutput(pipelinePhase.getStagesOfType(Transform.PLUGIN_TYPE), sinkOutputs) ?
      new SingleOutputWriter<>(context) : new MultiOutputWriter<>(context, sinkOutputs);
  }

  private boolean hasSingleOutput(Set<StageInfo> transformInfos, Map<String, SinkOutput> sinkOutputs) {
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

  public void transform(KEY key, VALUE value) {
    try {
      KeyValue<KEY, VALUE> input = new KeyValue<>(key, value);
      TransformResponse transformResponse = transformExecutor.runOneIteration(input);
      for (Map.Entry<String, Collection<Object>> transformedEntry : transformResponse.getSinksResults().entrySet()) {
        for (Object transformedRecord : transformedEntry.getValue()) {
          outputWriter.write(transformedEntry.getKey(), (KeyValue<Object, Object>) transformedRecord);
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

  public void destroy() {
    Destroyables.destroyQuietly(transformExecutor);
  }
}
