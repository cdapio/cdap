/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchConfigurable;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.batch.connector.MultiConnectorFactory;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.submit.Finisher;
import io.cdap.cdap.etl.common.submit.SubmitterPlugin;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * For each stage, call prepareRun() in topological order.
 * prepareRun will setup the input/output of the pipeline phase and set any arguments that should be visible
 * to subsequent stages. These configure and prepare operations must be performed in topological order
 * to ensure that arguments set by one stage are available to subsequent stages.
 *
 */
public abstract class PipelinePhasePreparer {
  private final PluginContext pluginContext;
  private final Metrics metrics;
  protected final MacroEvaluator macroEvaluator;
  protected final PipelineRuntime pipelineRuntime;

  public PipelinePhasePreparer(PluginContext pluginContext, Metrics metrics, MacroEvaluator macroEvaluator,
                               PipelineRuntime pipelineRuntime) {
    this.pluginContext = pluginContext;
    this.metrics = metrics;
    this.macroEvaluator = macroEvaluator;
    this.pipelineRuntime = pipelineRuntime;
  }

  /**
   * Prepare all the stages in the given phase and return Finishers that must be run when the pipeline completes.
   *
   * @param phaseSpec the pipeline phase to prepare
   * @return list of finishers that should be run when the pipeline ends
   */
  public List<Finisher> prepare(BatchPhaseSpec phaseSpec)
    throws TransactionFailureException, InstantiationException, IOException {
    PipelinePluginInstantiator pluginInstantiator =
      new PipelinePluginInstantiator(pluginContext, metrics, phaseSpec, new MultiConnectorFactory());
    PipelinePhase phase = phaseSpec.getPhase();

    List<Finisher> finishers = new ArrayList<>();
    // call prepareRun on each stage in order so that any arguments set by a stage will be visible to subsequent stages
    for (String stageName : phase.getDag().getTopologicalOrder()) {
      StageSpec stageSpec = phase.getStage(stageName);
      String pluginType = stageSpec.getPluginType();
      boolean isConnectorSource =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSources().contains(stageName);
      boolean isConnectorSink =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSinks().contains(stageName);

      SubmitterPlugin submitterPlugin;
      if (BatchSource.PLUGIN_TYPE.equals(pluginType) || isConnectorSource) {
        BatchConfigurable<BatchSourceContext> batchSource =
          pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        submitterPlugin = createSource(batchSource, stageSpec);
      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType) || AlertPublisher.PLUGIN_TYPE.equals(pluginType) ||
        isConnectorSink) {
        BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        submitterPlugin = createSink(batchSink, stageSpec);
      } else if (Transform.PLUGIN_TYPE.equals(pluginType) || ErrorTransform.PLUGIN_TYPE.equals(pluginType)) {
        Transform<?, ?> transform = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        submitterPlugin = createTransform(transform, stageSpec);
      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
        BatchAggregator<?, ?, ?> aggregator = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        submitterPlugin = createAggregator(aggregator, stageSpec);
      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
        BatchJoiner<?, ?, ?> batchJoiner = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        submitterPlugin = createJoiner(batchJoiner, stageSpec);
      } else {
        submitterPlugin = create(pluginInstantiator, stageSpec);
      }

      if (submitterPlugin != null) {
        submitterPlugin.prepareRun();
        finishers.add(submitterPlugin);
      }
    }

    return finishers;
  }

  @Nullable
  protected abstract SubmitterPlugin create(PipelinePluginInstantiator pluginInstantiator, StageSpec stageSpec)
    throws InstantiationException;

  protected abstract SubmitterPlugin createSource(BatchConfigurable<BatchSourceContext> batchSource,
                                                  StageSpec stageSpec);

  protected abstract SubmitterPlugin createSink(BatchConfigurable<BatchSinkContext> batchSink, StageSpec stageSpec);

  protected abstract SubmitterPlugin createTransform(Transform<?, ?> transform, StageSpec stageSpec);

  protected abstract SubmitterPlugin createAggregator(BatchAggregator<?, ?, ?> aggregator, StageSpec stageSpec);

  protected abstract SubmitterPlugin createJoiner(BatchJoiner<?, ?, ?> batchJoiner, StageSpec stageSpec);

}
