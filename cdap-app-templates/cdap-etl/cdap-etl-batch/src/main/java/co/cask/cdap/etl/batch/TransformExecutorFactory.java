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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.etl.api.StageLifecycle;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchEmitter;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.batch.mapreduce.BatchTransformExecutor;
import co.cask.cdap.etl.batch.mapreduce.ConnectorSourceEmitter;
import co.cask.cdap.etl.batch.mapreduce.ErrorOutputWriter;
import co.cask.cdap.etl.batch.mapreduce.OutputWriter;
import co.cask.cdap.etl.batch.mapreduce.SinkEmitter;
import co.cask.cdap.etl.batch.mapreduce.TransformEmitter;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public abstract class TransformExecutorFactory<T> {
  protected final Map<String, Map<String, Schema>> perStageInputSchemas;
  private final String sourceStageName;
  private final MacroEvaluator macroEvaluator;
  protected final PipelinePluginInstantiator pluginInstantiator;
  protected final Metrics metrics;
  protected Map<String, Schema> outputSchemas;
  protected boolean isMapPhase;

  public TransformExecutorFactory(JobContext hadoopContext, PipelinePluginInstantiator pluginInstantiator,
                                  Metrics metrics, @Nullable String sourceStageName, MacroEvaluator macroEvaluator) {
    this.pluginInstantiator = pluginInstantiator;
    this.metrics = metrics;
    this.perStageInputSchemas = new HashMap<>();
    this.outputSchemas = new HashMap<>();
    this.sourceStageName = sourceStageName;
    this.macroEvaluator = macroEvaluator;
    this.isMapPhase = hadoopContext instanceof Mapper.Context;
  }

  protected abstract BatchRuntimeContext createRuntimeContext(String stageName);

  protected abstract TrackedTransform getTransformation(String pluginType, String stageName)
    throws Exception;

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception              if there was an error initializing a plugin
   */
  public <KEY_OUT, VAL_OUT> BatchTransformExecutor<T> create(PipelinePhase pipeline,
                                                             OutputWriter<KEY_OUT, VAL_OUT> outputWriter,
                                                             Map<String, ErrorOutputWriter<Object, Object>>
                                                               transformErrorSinkMap)
    throws Exception {
    Map<String, BatchTransformDetail> transformations = new HashMap<>();
    Set<String> sources = pipeline.getSources();

    // Set input and output schema for this stage
    for (String pluginType : pipeline.getPluginTypes()) {
      for (StageInfo stageInfo : pipeline.getStagesOfType(pluginType)) {
        String stageName = stageInfo.getName();
        outputSchemas.put(stageName, stageInfo.getOutputSchema());
        perStageInputSchemas.put(stageName, stageInfo.getInputSchemas());
      }
    }

    // recursively set BatchTransformDetail for all the stages
    for (String source : sources) {
      setBatchTransformDetail(pipeline, source, transformations, transformErrorSinkMap, outputWriter);
    }

    // sourceStageName will be null in reducers, so need to handle that case
    Set<String> startingPoints = (sourceStageName == null) ? pipeline.getSources() : Sets.newHashSet(sourceStageName);
    return new BatchTransformExecutor<>(transformations, startingPoints);
  }

  private <KEY_OUT, VAL_OUT> void setBatchTransformDetail(PipelinePhase pipeline, String stageName,
                                                          Map<String, BatchTransformDetail> transformations,
                                                          Map<String, ErrorOutputWriter<Object, Object>>
                                                            transformErrorSinkMap,
                                                          OutputWriter<KEY_OUT, VAL_OUT> outputWriter)
    throws Exception {
    if (pipeline.getSinks().contains(stageName)) {
      String pluginType = pipeline.getStage(stageName).getPluginType();
      // If there is a connector sink/ joiner at the end of pipeline, do not remove stage name. This is needed to save
      // stageName along with the record in connector sink and joiner takes input along with stageName
      boolean removeStageName = !(pluginType.equalsIgnoreCase(Constants.CONNECTOR_TYPE) ||
        pluginType.equalsIgnoreCase(BatchJoiner.PLUGIN_TYPE));
      transformations.put(stageName, new BatchTransformDetail(stageName, removeStageName,
                                                              getTransformation(pluginType, stageName),
                                                              new SinkEmitter<>(stageName, outputWriter)));
      return;
    }

    for (String output : pipeline.getDag().getNodeOutputs(stageName)) {
      setBatchTransformDetail(pipeline, output, transformations, transformErrorSinkMap, outputWriter);

      // Add new transformation for output stage if the transfomation for stageName already exists
      if (transformations.containsKey(stageName)) {
        BatchEmitter<BatchTransformDetail> emitter = transformations.get(stageName).getEmitter();
        Map<String, BatchTransformDetail> nextStages = emitter.getNextStages();
        if (nextStages != null && !nextStages.containsKey(output)) {
          emitter.addTransformDetail(output, transformations.get(output));
        }
      } else {
        HashMap<String, BatchTransformDetail> map = new HashMap<>();
        map.put(output, transformations.get(output));
        StageInfo stageInfo = pipeline.getStage(stageName);
        String pluginType = stageInfo.getPluginType();
        ErrorOutputWriter<Object, Object> errorOutputWriter = transformErrorSinkMap.containsKey(stageName) ?
          transformErrorSinkMap.get(stageName) : null;
        // If stageName is a connector source, it will have stageName along with record so use ConnectorSourceEmitter
        if (pipeline.getSources().contains(stageName) && pluginType.equalsIgnoreCase(Constants.CONNECTOR_TYPE)) {
          transformations.put(stageName,
                              new BatchTransformDetail(stageName, true,
                                                       getTransformation(pluginType, stageName),
                                                       new ConnectorSourceEmitter(stageName, map)));
        } else if (pluginType.equalsIgnoreCase(BatchJoiner.PLUGIN_TYPE) && isMapPhase) {
          // Do not remove stageName only for Map phase of BatchJoiner
          transformations.put(stageName,
                              new BatchTransformDetail(stageName, false,
                                                       getTransformation(pluginType, stageName),
                                                       new TransformEmitter(stageName, map, errorOutputWriter)));
        } else {
          transformations.put(stageName,
                              new BatchTransformDetail(stageName, true,
                                                       getTransformation(pluginType, stageName),
                                                       new TransformEmitter(stageName, map, errorOutputWriter)));
        }
      }
    }
  }

  /**
   * Instantiates and initializes the plugin for the stage.
   *
   * @param stageName the stage name.
   * @return the initialized Transformation
   * @throws InstantiationException if the plugin for the stage could not be instantiated
   * @throws Exception              if there was a problem initializing the plugin
   */
  protected <T extends Transformation & StageLifecycle<BatchRuntimeContext>> Transformation
  getInitializedTransformation(String stageName) throws Exception {
    BatchRuntimeContext runtimeContext = createRuntimeContext(stageName);
    T plugin = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
    plugin.initialize(runtimeContext);
    return plugin;
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedEmitKeyStep(Transformation<IN, OUT> transform,
                                                                             StageMetrics stageMetrics,
                                                                             DataTracer dataTracer) {
    return new TrackedTransform<>(transform, stageMetrics, TrackedTransform.RECORDS_IN, null, dataTracer,
                                  TrackedTransform.RECORDS_IN);
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedAggregateStep(Transformation<IN, OUT> transform,
                                                                               StageMetrics stageMetrics,
                                                                               DataTracer dataTracer) {
    // 'aggregator.groups' is the number of groups output by the aggregator
    return new TrackedTransform<>(transform, stageMetrics, "aggregator.groups", TrackedTransform.RECORDS_OUT,
                                  dataTracer, null);
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedMergeStep(Transformation<IN, OUT> transform,
                                                                           StageMetrics stageMetrics,
                                                                           DataTracer dataTracer) {
    return new TrackedTransform<>(transform, stageMetrics, null, TrackedTransform.RECORDS_OUT, dataTracer, null);
  }
}
