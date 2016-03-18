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
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.TransformExecutorFactory;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TransformExecutor;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;
import java.util.Map;

/**
 * Sets up and runs transforms in the mapper portion of a pipeline phase.
 */
public class ReducerTransformRunner extends TransformRunner<Object, Iterator> {

  public ReducerTransformRunner(MapReduceTaskContext<Object, Object> context,
                                Metrics metrics) throws Exception {
    super(context, ((Reducer.Context) context.getHadoopContext()).getConfiguration(), metrics);
  }

  @Override
  protected TransformExecutor<KeyValue<Object, Iterator>> getTransformExecutor(
    MapReduceTaskContext<Object, Object> context,
    PipelinePluginInstantiator pluginInstantiator,
    Metrics metrics,
    Map<String, Map<String, String>> runtimeArgs,
    PipelinePhase pipelinePhase) throws Exception {

    String aggregatorName = pipelinePhase.getStagesOfType(BatchAggregator.PLUGIN_TYPE).iterator().next().getName();
    PipelinePhase reducerPipeline = pipelinePhase.subsetFrom(ImmutableSet.of(aggregatorName));

    TransformExecutorFactory<KeyValue<Object, Iterator>> transformExecutorFactory =
      new ReducerTransformExecutorFactory<>(context, pluginInstantiator, metrics, runtimeArgs);
    return transformExecutorFactory.create(reducerPipeline);
  }
}
