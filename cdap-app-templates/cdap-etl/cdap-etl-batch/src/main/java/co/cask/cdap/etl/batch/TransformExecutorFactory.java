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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.LoggedTransform;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TransformDetail;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public abstract class TransformExecutorFactory<T> {
  protected final PluginContext pluginContext;
  protected final Metrics metrics;
  protected final long logicalStartTime;

  public TransformExecutorFactory(PluginContext pluginContext, Metrics metrics, long logicalStartTime) {
    this.pluginContext = pluginContext;
    this.metrics = metrics;
    this.logicalStartTime = logicalStartTime;
  }

  protected abstract BatchRuntimeContext createRuntimeContext(String stageName);

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception if there was an error initializing a plugin
   */
  public TransformExecutor<T> create(PipelinePhase pipeline) throws Exception {
    Map<String, Set<String>> connections = pipeline.getConnections();
    String sourceName = pipeline.getSource().getName();
    BatchSource<?, ?, ?> source = pluginContext.newPluginInstance(sourceName);
    source = new LoggedBatchSource<>(sourceName, source);
    BatchRuntimeContext runtimeContext = createRuntimeContext(sourceName);
    source.initialize(runtimeContext);

    Map<String, TransformDetail> transformations = new HashMap<>();
    transformations.put(sourceName, new TransformDetail(
      source, new DefaultStageMetrics(metrics, sourceName), connections.get(sourceName)));
    addTransforms(transformations, pipeline.getTransforms(), connections);

    for (StageInfo sinkInfo : pipeline.getSinks()) {
      String sinkName = sinkInfo.getName();
      BatchSink<?, ?, ?> batchSink = pluginContext.newPluginInstance(sinkName);
      batchSink = new LoggedBatchSink<>(sinkName, batchSink);
      BatchRuntimeContext sinkContext = createRuntimeContext(sinkName);
      batchSink.initialize(sinkContext);
      transformations.put(sinkInfo.getName(), new TransformDetail(
        batchSink, new DefaultStageMetrics(metrics, sinkInfo.getName()), new ArrayList<String>()));
    }

    return new TransformExecutor<>(transformations, ImmutableList.of(sourceName));
  }

  private void addTransforms(Map<String, TransformDetail> transformations,
                             Set<StageInfo> transformInfos,
                             Map<String, Set<String>> connections) throws Exception {
    for (StageInfo transformInfo : transformInfos) {
      String transformName = transformInfo.getName();
      Transform<?, ?> transform = pluginContext.newPluginInstance(transformName);
      transform = new LoggedTransform<>(transformName, transform);
      BatchRuntimeContext transformContext = createRuntimeContext(transformName);
      transform.initialize(transformContext);
      transformations.put(transformName, new TransformDetail(
        transform, new DefaultStageMetrics(metrics, transformName), connections.get(transformName)));
    }
  }
}
