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
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Map;
import java.util.Set;

/**
 * Sets up and runs transforms in the mapper portion of a pipeline phase.
 */
public class MapperTransformRunner extends TransformRunner<Object, Object> {
  public MapperTransformRunner(MapReduceTaskContext<Object, Object> context,
                               Metrics metrics) throws Exception {
    super(context, ((Mapper.Context) context.getHadoopContext()).getConfiguration(), metrics);
  }

  @Override
  protected TransformExecutor<KeyValue<Object, Object>> getTransformExecutor(
    MapReduceTaskContext<Object, Object> context,
    PipelinePluginInstantiator pluginInstantiator,
    Metrics metrics,
    Map<String, Map<String, String>> runtimeArgs,
    PipelinePhase pipelinePhase) throws Exception {

    TransformExecutorFactory<KeyValue<Object, Object>> transformExecutorFactory =
      new MapperTransformExecutorFactory<>(context, pluginInstantiator, metrics, runtimeArgs);

    Set<StageInfo> aggregators = pipelinePhase.getStagesOfType(BatchAggregator.PLUGIN_TYPE);
    if (!aggregators.isEmpty()) {
      String aggregatorName = aggregators.iterator().next().getName();
      return transformExecutorFactory.create(pipelinePhase.subsetTo(ImmutableSet.of(aggregatorName)));
    }
    return transformExecutorFactory.create(pipelinePhase);
  }
}
