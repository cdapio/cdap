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
import co.cask.cdap.etl.api.StageLifecycle;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TransformDetail;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public abstract class TransformExecutorFactory<T> {
  protected final PipelinePluginInstantiator pluginInstantiator;
  protected final Metrics metrics;

  public TransformExecutorFactory(PipelinePluginInstantiator pluginInstantiator, Metrics metrics) {
    this.pluginInstantiator = pluginInstantiator;
    this.metrics = metrics;
  }

  protected abstract BatchRuntimeContext createRuntimeContext(String stageName);

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @param pluginTypes which plugin types are Transforms
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception if there was an error initializing a plugin
   */
  public TransformExecutor<T> create(PipelinePhase pipeline, Set<String> pluginTypes) throws Exception {

    Map<String, TransformDetail> transformations = new HashMap<>();
    for (String pluginType : pluginTypes) {
      for (StageInfo transformInfo : pipeline.getStagesOfType(pluginType)) {
        String transformName = transformInfo.getName();
        TransformDetail transformDetail = new TransformDetail(
          getInitializedTransformation(transformName),
          new DefaultStageMetrics(metrics, transformName),
          pipeline.getStageOutputs(transformName));
        transformations.put(transformName, transformDetail);
      }
    }

    return new TransformExecutor<>(transformations, pipeline.getSources());
  }

  /**
   * Instantiates and initializes the plugin for the stage.
   *
   * @param stageName the stage name.
   * @return the initialized Transformation
   * @throws InstantiationException if the plugin for the stage could not be instantiated
   * @throws Exception if there was a problem initializing the plugin
   */
  private <T extends Transformation & StageLifecycle<BatchRuntimeContext>> Transformation
  getInitializedTransformation(String stageName) throws Exception {
    BatchRuntimeContext runtimeContext = createRuntimeContext(stageName);
    T plugin = pluginInstantiator.newPluginInstance(stageName);
    plugin.initialize(runtimeContext);
    return plugin;
  }
}
