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

import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.batch.TransformExecutorFactory;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates transform executors for mapreduce programs.
 *
 * @param <T> the type of input for the created transform executors
 */
public class MapReduceTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final Map<String, Map<String, String>> pluginRuntimeArgs;
  private final MapReduceTaskContext taskContext;

  public MapReduceTransformExecutorFactory(MapReduceTaskContext taskContext, Metrics metrics, long logicalStartTime,
                                           Map<String, Map<String, String>> pluginRuntimeArgs) {
    super(taskContext, metrics, logicalStartTime);
    this.taskContext = taskContext;
    this.pluginRuntimeArgs = pluginRuntimeArgs;
  }

  @Override
  protected BatchRuntimeContext createRuntimeContext(String stageName) {
    Map<String, String> stageRuntimeArgs = pluginRuntimeArgs.get(stageName);
    if (stageRuntimeArgs == null) {
      stageRuntimeArgs = new HashMap<>();
    }
    return new MapReduceRuntimeContext(taskContext, metrics, new DatasetContextLookupProvider(taskContext),
                                       stageName, stageRuntimeArgs);
  }
}
