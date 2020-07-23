/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.exec;

import com.google.common.collect.Sets;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.StageLifecycle;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.batch.PipelinePluginInstantiator;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.NoopStageStatisticsCollector;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.TrackedMultiOutputTransform;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.common.TransformExecutor;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public abstract class TransformExecutorFactory<T> {
  protected final String sourceStageName;
  protected final MacroEvaluator macroEvaluator;
  protected final PipelinePluginInstantiator pluginInstantiator;
  protected final Metrics metrics;
  protected final boolean collectStageStatistics;

  protected TransformExecutorFactory(PipelinePluginInstantiator pluginInstantiator, MacroEvaluator macroEvaluator,
                                     Metrics metrics, String sourceStageName,
                                     boolean collectStageStatistics) {
    this.pluginInstantiator = pluginInstantiator;
    this.metrics = metrics;
    this.sourceStageName = sourceStageName;
    this.macroEvaluator = macroEvaluator;
    this.collectStageStatistics = collectStageStatistics;
  }

  protected abstract DataTracer getDataTracer(String stageName);

  protected abstract StageStatisticsCollector getStatisticsCollector(String stageName);

  protected abstract BatchRuntimeContext createRuntimeContext(StageSpec stageSpec);

  protected abstract PipeStage getSinkPipeStage(StageSpec stageSpec) throws Exception;

  private <IN, ERROR> TrackedMultiOutputTransform<IN, ERROR> getMultiOutputTransform(StageSpec stageSpec)
    throws Exception {
    String stageName = stageSpec.getName();
    SplitterTransform<IN, ERROR> splitterTransform =
      pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
    TransformContext transformContext = createRuntimeContext(stageSpec);
    splitterTransform.initialize(transformContext);

    StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
    StageStatisticsCollector collector = collectStageStatistics ?
      getStatisticsCollector(stageName) : NoopStageStatisticsCollector.INSTANCE;
    return new TrackedMultiOutputTransform<>(splitterTransform, stageMetrics, getDataTracer(stageName), collector);
  }

  @SuppressWarnings("unchecked")
  protected <IN, OUT> TrackedTransform<IN, OUT> getTransformation(StageSpec stageSpec) throws Exception {

    String stageName = stageSpec.getName();
    String pluginType = stageSpec.getPluginType();
    StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
    StageStatisticsCollector collector = collectStageStatistics ?
      getStatisticsCollector(stageName) : NoopStageStatisticsCollector.INSTANCE;

    Transformation transformation = getInitializedTransformation(stageSpec);
    // we emit metrics for records into alert publishers when the actual alerts are published,
    // not when we write the alerts to the temporary dataset
    String recordsInMetric = AlertPublisher.PLUGIN_TYPE.equals(pluginType) ? null : Constants.Metrics.RECORDS_IN;
    return new TrackedTransform<>(transformation, stageMetrics, recordsInMetric, Constants.Metrics.RECORDS_OUT,
                                  getDataTracer(stageName), collector);
  }

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception              if there was an error initializing a plugin
   */
  public PipeTransformExecutor<T> create(PipelinePhase pipeline) throws Exception {
    // populate the pipe stages in reverse topological order to ensure that an output is always created before its
    // input. this will allow us to setup all outputs for a stage when we get to it.
    List<String> traversalOrder = pipeline.getDag().getTopologicalOrder();
    Collections.reverse(traversalOrder);

    Map<String, PipeStage> pipeStages = new HashMap<>();
    for (String stageName : traversalOrder) {
      pipeStages.put(stageName, getPipeStage(pipeline, stageName, pipeStages));
    }

    // sourceStageName will be null in reducers, so need to handle that case
    Set<String> startingPoints = (sourceStageName == null) ? pipeline.getSources() : Sets.newHashSet(sourceStageName);
    return new PipeTransformExecutor<>(pipeStages, startingPoints);
  }

  private PipeStage getPipeStage(PipelinePhase pipeline, String stageName,
                                 Map<String, PipeStage> pipeStages) throws Exception {
    StageSpec stageSpec = pipeline.getStage(stageName);
    String pluginType = stageSpec.getPluginType();

    /*
     * Every stage in the pipe uses a PipeEmitter except for ending stages, which use an emitter that
     * lets them write directly to the MapReduce context instead of some other stage.
     *
     * Since every PipeEmitter outputs RecordInfo because some stages require the stage the record was from.
     * Connector sinks require the stage name so that they can preserve it for the next phase.
     * Joiners inside mappers require it so that they can include the stage name with each record, so that on the
     * reduce side, Joiner.join() can be sent records along with the stage they came from.
     *
     * PipeEmitter emits ErrorRecord for ErrorTransforms.
     *
     * Every other non-starting stage in the pipe takes RecordInfo as input.
     * These stages need to be wrapped so that they extract the actual
     * value inside a RecordInfo before passing it to the actual plugin.
     */

    // handle ending stage case, which don't use PipeEmitter
    if (pipeline.getSinks().contains(stageName)) {
      return getSinkPipeStage(stageSpec);
    }

    // create PipeEmitter, which holds all output PipeStages it needs to write to and wraps any output it gets
    // into a RecordInfo
    // ConnectorSources require a special emitter since they need to build RecordInfo from the temporary dataset
    PipeEmitter.Builder emitterBuilder =
      Constants.Connector.PLUGIN_TYPE.equals(pluginType) && pipeline.getSources().contains(stageName) ?
        ConnectorSourceEmitter.builder(stageName) : PipeEmitter.builder(stageName);

    Map<String, StageSpec.Port> outputPorts = stageSpec.getOutputPorts();
    for (String outputStageName : pipeline.getDag().getNodeOutputs(stageName)) {
      StageSpec outputStageSpec = pipeline.getStage(outputStageName);
      String outputStageType = outputStageSpec.getPluginType();
      PipeStage outputPipeStage = pipeStages.get(outputStageName);

      if (ErrorTransform.PLUGIN_TYPE.equals(outputStageType)) {
        emitterBuilder.addErrorConsumer(outputPipeStage);
      } else if (AlertPublisher.PLUGIN_TYPE.equals(outputStageType)) {
        emitterBuilder.addAlertConsumer(outputPipeStage);
      } else if (Constants.Connector.PLUGIN_TYPE.equals(pluginType)) {
        // connectors only have a single output
        emitterBuilder.addOutputConsumer(outputPipeStage);
      } else {
        // if the output is a connector like agg5.connector, the outputPorts will contain the original 'agg5' as
        // a key, but not 'agg5.connector' so we need to lookup the original stage from the connector's plugin spec
        String originalOutputName = Constants.Connector.PLUGIN_TYPE.equals(outputStageType) ?
          outputStageSpec.getPlugin().getProperties().get(Constants.Connector.ORIGINAL_NAME) : outputStageName;

        String port = outputPorts.containsKey(originalOutputName) ? outputPorts.get(originalOutputName).getPort()
          : null;
        if (port != null) {
          emitterBuilder.addOutputConsumer(outputPipeStage, port);
        } else {
          emitterBuilder.addOutputConsumer(outputPipeStage);
        }
      }
    }
    PipeEmitter pipeEmitter = emitterBuilder.build();

    if (SplitterTransform.PLUGIN_TYPE.equals(pluginType)) {
      // this is a SplitterTransform, needs to emit records to the right outputs based on port
      return new MultiOutputTransformPipeStage<>(stageName, getMultiOutputTransform(stageSpec), pipeEmitter);
    } else {
      return new UnwrapPipeStage<>(stageName, getTransformation(stageSpec), pipeEmitter);
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
  private <T extends Transformation & StageLifecycle<BatchRuntimeContext>> Transformation
  getInitializedTransformation(StageSpec stageInfo) throws Exception {
    BatchRuntimeContext runtimeContext = createRuntimeContext(stageInfo);
    T plugin = pluginInstantiator.newPluginInstance(stageInfo.getName(), macroEvaluator);
    plugin.initialize(runtimeContext);
    return plugin;
  }

}
