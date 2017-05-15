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
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.StageLifecycle;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.batch.mapreduce.ConnectorSourceEmitter;
import co.cask.cdap.etl.batch.mapreduce.ErrorOutputWriter;
import co.cask.cdap.etl.batch.mapreduce.OutputWriter;
import co.cask.cdap.etl.batch.mapreduce.PipeTransformExecutor;
import co.cask.cdap.etl.batch.mapreduce.SinkEmitter;
import co.cask.cdap.etl.batch.mapreduce.TransformEmitter;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  protected final Map<String, Schema> outputSchemas;
  protected boolean isMapPhase;
  private static final Logger PIPELINE_LOG =
    new LocationAwareMDCWrapperLogger(LoggerFactory.getLogger(TransformExecutorFactory.class), Constants.EVENT_TYPE_TAG,
                                      Constants.PIPELINE_LIFECYCLE_TAG_VALUE);

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

  protected abstract BatchRuntimeContext createRuntimeContext(StageInfo stageInfo);

  protected abstract TrackedTransform getTransformation(StageInfo stageInfo) throws Exception;

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception              if there was an error initializing a plugin
   */
  public <KEY_OUT, VAL_OUT> PipeTransformExecutor<T> create(PipelinePhase pipeline,
                                                            OutputWriter<KEY_OUT, VAL_OUT> outputWriter,
                                                            Map<String, ErrorOutputWriter<Object, Object>>
                                                              transformErrorSinkMap)
    throws Exception {
    Map<String, PipeTransformDetail> transformations = new HashMap<>();
    Set<String> sources = pipeline.getSources();

    // Set input and output schema for this stage
    for (String pluginType : pipeline.getPluginTypes()) {
      for (StageInfo stageInfo : pipeline.getStagesOfType(pluginType)) {
        String stageName = stageInfo.getName();
        outputSchemas.put(stageName, stageInfo.getOutputSchema());
        perStageInputSchemas.put(stageName, stageInfo.getInputSchemas());
      }
    }

    // recursively set PipeTransformDetail for all the stages
    for (String source : sources) {
      setPipeTransformDetail(pipeline, source, transformations, transformErrorSinkMap, outputWriter);
    }

    // sourceStageName will be null in reducers, so need to handle that case
    Set<String> startingPoints = (sourceStageName == null) ? pipeline.getSources() : Sets.newHashSet(sourceStageName);
    return new PipeTransformExecutor<>(transformations, startingPoints);
  }

  private <KEY_OUT, VAL_OUT> void setPipeTransformDetail(PipelinePhase pipeline, String stageName,
                                                         Map<String, PipeTransformDetail> transformations,
                                                         Map<String, ErrorOutputWriter<Object, Object>>
                                                           transformErrorSinkMap,
                                                         OutputWriter<KEY_OUT, VAL_OUT> outputWriter)
    throws Exception {
    if (pipeline.getSinks().contains(stageName)) {
      StageInfo stageInfo = pipeline.getStage(stageName);
      // If there is a connector sink/ joiner at the end of pipeline, do not remove stage name. This is needed to save
      // stageName along with the record in connector sink and joiner takes input along with stageName
      String pluginType = stageInfo.getPluginType();
      boolean removeStageName = !(pluginType.equals(Constants.CONNECTOR_TYPE) ||
        pluginType.equals(BatchJoiner.PLUGIN_TYPE));
      boolean isErrorConsumer = pluginType.equals(ErrorTransform.PLUGIN_TYPE);
      transformations.put(stageName, new PipeTransformDetail(stageName, removeStageName, isErrorConsumer,
                                                             getTransformation(stageInfo),
                                                             new SinkEmitter<>(stageName, outputWriter)));
      return;
    }

    try {
      addTransformation(pipeline, stageName, transformations, transformErrorSinkMap);
    } catch (Exception e) {
      // Catch the Exception to generate a User Error Log for the Pipeline
      PIPELINE_LOG.error("Failed to start pipeline stage '{}' with the error: {}. Please review your pipeline " +
                          "configuration and check the system logs for more details.", stageName,
                        Throwables.getRootCause(e).getMessage(), Throwables.getRootCause(e));
      throw e;
    }

    for (String output : pipeline.getDag().getNodeOutputs(stageName)) {
      setPipeTransformDetail(pipeline, output, transformations, transformErrorSinkMap, outputWriter);
      transformations.get(stageName).addTransformation(output, transformations.get(output));
    }
  }

  private void addTransformation(PipelinePhase pipeline, String stageName,
                                 Map<String, PipeTransformDetail> transformations,
                                 Map<String, ErrorOutputWriter<Object, Object>> transformErrorSinkMap)
    throws Exception {
    StageInfo stageInfo = pipeline.getStage(stageName);
    String pluginType = stageInfo.getPluginType();
    ErrorOutputWriter<Object, Object> errorOutputWriter = transformErrorSinkMap.containsKey(stageName) ?
      transformErrorSinkMap.get(stageName) : null;

    // If stageName is a connector source, it will have stageName along with record so use ConnectorSourceEmitter
    if (pipeline.getSources().contains(stageName) && pluginType.equals(Constants.CONNECTOR_TYPE)) {
      transformations.put(stageName,
                          new PipeTransformDetail(stageName, true, false,
                                                  getTransformation(stageInfo),
                                                  new ConnectorSourceEmitter(stageName)));
    } else if (pluginType.equals(BatchJoiner.PLUGIN_TYPE) && isMapPhase) {
      // Do not remove stageName only for Map phase of BatchJoiner
      transformations.put(stageName,
                          new PipeTransformDetail(stageName, false, false,
                                                  getTransformation(stageInfo),
                                                  new TransformEmitter(stageName, errorOutputWriter)));
    } else {
      boolean isErrorConsumer = ErrorTransform.PLUGIN_TYPE.equals(pluginType);
      transformations.put(stageName,
                          new PipeTransformDetail(stageName, true, isErrorConsumer,
                                                  getTransformation(stageInfo),
                                                  new TransformEmitter(stageName, errorOutputWriter)));
    }
  }

  /**
   * Instantiates and initializes the plugin for the stage.
   *
   * @param stageInfo the stage info
   * @return the initialized Transformation
   * @throws InstantiationException if the plugin for the stage could not be instantiated
   * @throws Exception              if there was a problem initializing the plugin
   */
  protected <T extends Transformation & StageLifecycle<BatchRuntimeContext>> Transformation
  getInitializedTransformation(StageInfo stageInfo) throws Exception {
    BatchRuntimeContext runtimeContext = createRuntimeContext(stageInfo);
    T plugin = pluginInstantiator.newPluginInstance(stageInfo.getName(), macroEvaluator);
    plugin.initialize(runtimeContext);
    return plugin;
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedEmitKeyStep(Transformation<IN, OUT> transform,
                                                                             StageMetrics stageMetrics,
                                                                             DataTracer dataTracer) {
    return new TrackedTransform<>(transform, stageMetrics, TrackedTransform.RECORDS_IN, null, dataTracer);
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedAggregateStep(Transformation<IN, OUT> transform,
                                                                               StageMetrics stageMetrics,
                                                                               DataTracer dataTracer) {
    // 'aggregator.groups' is the number of groups output by the aggregator
    return new TrackedTransform<>(transform, stageMetrics, "aggregator.groups", TrackedTransform.RECORDS_OUT,
                                  dataTracer);
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedMergeStep(Transformation<IN, OUT> transform,
                                                                           StageMetrics stageMetrics,
                                                                           DataTracer dataTracer) {
    return new TrackedTransform<>(transform, stageMetrics, null, TrackedTransform.RECORDS_OUT, dataTracer);
  }
}
