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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.connector.MultiConnectorFactory;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.etl.common.DefaultStageConfigurer;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.RuntimePluginDatasetConfigurer;
import co.cask.cdap.etl.common.submit.Finisher;
import co.cask.cdap.etl.common.submit.SubmitterPlugin;
import co.cask.cdap.etl.proto.v2.spec.StageSpec;
import co.cask.cdap.etl.spec.SchemaPropagator;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
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
  private final Engine engine;
  private final Admin admin;
  protected final MacroEvaluator macroEvaluator;
  protected final PipelineRuntime pipelineRuntime;

  public PipelinePhasePreparer(PluginContext pluginContext, Admin admin, Metrics metrics, MacroEvaluator macroEvaluator,
                               PipelineRuntime pipelineRuntime, Engine engine) {
    this.pluginContext = pluginContext;
    this.metrics = metrics;
    this.macroEvaluator = macroEvaluator;
    this.pipelineRuntime = pipelineRuntime;
    this.engine = engine;
    this.admin = admin;
  }

  /**
   * Prepare all the stages in the given phase and return Finishers that must be run when the pipeline completes.
   *
   * @param phaseSpec the pipeline phase to prepare
   * @return list of finishers that should be run when the pipeline ends
   */
  public PreparedPipelinePhase prepare(BatchPhaseSpec phaseSpec, WorkflowToken token)
    throws TransactionFailureException, InstantiationException, IOException {
    PipelinePluginInstantiator pluginInstantiator =
      new PipelinePluginInstantiator(pluginContext, metrics, phaseSpec, new MultiConnectorFactory());
    PipelinePhase phase = phaseSpec.getPhase();

    Map<String, DefaultPipelineConfigurer> pipelineConfigurers = new HashMap<>(phase.getDag().getNodes().size());
    for (String stageName : phase.getDag().getNodes()) {
      RuntimePluginDatasetConfigurer configurer =
        new RuntimePluginDatasetConfigurer(stageName, admin, pluginContext, macroEvaluator);
      pipelineConfigurers.put(stageName, new DefaultPipelineConfigurer(configurer, stageName, engine));
    }
    SchemaPropagator schemaPropagator = new SchemaPropagator(
      pipelineConfigurers,
      phase::getStageOutputs,
      stageName -> phase.getStage(stageName).getPluginType(),
      connectorName -> phase.getStage(connectorName).getPlugin().getProperties().get(Constants.Connector.ORIGINAL_NAME),
      token);

    List<Finisher> finishers = new ArrayList<>();
    // for each stage, call configurePipeline() and then prepareRun()
    // configurePipeline() will perform validation, dataset creation, and schema propagation
    // that was not be performed at deployment time due to macro evaluation
    // prepareRun will setup the input/output of the pipeline phase and set any arguments that should be visible
    // to subsequent stages. These configure and prepare operations must be performed in topological order
    // to ensure that arguments set by one stage are available to subsequent stages.
    Set<StageSpec> updatedStageSpecs = new HashSet<>(pipelineConfigurers.size());
    for (String stageName : phase.getDag().getTopologicalOrder()) {
      StageSpec stageSpec = phase.getStage(stageName);
      String pluginType = stageSpec.getPluginType();
      boolean isConnectorSource =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSources().contains(stageName);
      boolean isConnectorSink =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && phase.getSinks().contains(stageName);

      DefaultPipelineConfigurer pipelineConfigurer = pipelineConfigurers.get(stageName);
      SubmitterPlugin submitterPlugin;
      StageSpec updatedSpec = stageSpec;
      if (BatchSource.PLUGIN_TYPE.equals(pluginType) || isConnectorSource) {

        BatchConfigurable<BatchSourceContext> batchSource =
          pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator, batchSource::configurePipeline);
        submitterPlugin = createSource(batchSource, updatedSpec);

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType) || AlertPublisher.PLUGIN_TYPE.equals(pluginType) ||
        isConnectorSink) {

        BatchConfigurable<BatchSinkContext> batchSink = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator, batchSink::configurePipeline);
        submitterPlugin = createSink(batchSink, updatedSpec);

      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

        Transform<?, ?> transform = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator, transform::configurePipeline);
        submitterPlugin = createTransform(transform, updatedSpec);

      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

        BatchAggregator<?, ?, ?> aggregator = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator, aggregator::configurePipeline);
        submitterPlugin = createAggregator(aggregator, updatedSpec);

      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

        BatchJoiner<?, ?, ?> batchJoiner = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator, batchJoiner::configurePipeline);
        submitterPlugin = createJoiner(batchJoiner, updatedSpec);

      } else if (SplitterTransform.PLUGIN_TYPE.equals(pluginType)) {

        SplitterTransform<?, ?> splitter = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
        updatedSpec = configureStage(stageSpec, pipelineConfigurer, schemaPropagator, splitter::configurePipeline);
        // TODO: (CDAP-15044) SplitterTransform should implement SubmitterLifecycle
        submitterPlugin = null;

      } else {
        submitterPlugin = create(pluginInstantiator, stageSpec, pipelineConfigurer,
                                 schemaPropagator, updatedStageSpecs);
      }

      if (submitterPlugin != null) {
        submitterPlugin.prepareRun();
        finishers.add(submitterPlugin);
      }
      updatedStageSpecs.add(updatedSpec);
    }

    // create a new BatchPhaseSpec based on updated stage specs
    PipelinePhase updatedPhase = new PipelinePhase(updatedStageSpecs, phase.getDag());
    BatchPhaseSpec updatedPhaseSpec = new BatchPhaseSpec(phaseSpec, updatedPhase);
    return new PreparedPipelinePhase(updatedPhaseSpec, finishers);
  }

  @Nullable
  protected abstract SubmitterPlugin create(PipelinePluginInstantiator pluginInstantiator, StageSpec currentSpec,
                                            DefaultPipelineConfigurer pipelineConfigurer,
                                            SchemaPropagator schemaPropagator, Set<StageSpec> updatedSpecs)
    throws InstantiationException;

  protected abstract SubmitterPlugin createSource(BatchConfigurable<BatchSourceContext> batchSource,
                                                  StageSpec stageSpec);

  protected abstract SubmitterPlugin createSink(BatchConfigurable<BatchSinkContext> batchSink, StageSpec stageSpec);

  protected abstract SubmitterPlugin createTransform(Transform<?, ?> transform, StageSpec stageSpec);

  protected abstract SubmitterPlugin createAggregator(BatchAggregator<?, ?, ?> aggregator, StageSpec stageSpec);

  protected abstract SubmitterPlugin createJoiner(BatchJoiner<?, ?, ?> batchJoiner, StageSpec stageSpec);

  protected StageSpec configureStage(StageSpec currentSpec, DefaultPipelineConfigurer pipelineConfigurer,
                                     SchemaPropagator schemaPropagator,
                                     Consumer<DefaultPipelineConfigurer> configureConsumer) {
    configureConsumer.accept(pipelineConfigurer);

    DefaultStageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    StageSpec.Builder updatedSpec = StageSpec.builder(currentSpec)
      .setErrorSchema(stageConfigurer.getErrorSchema())
      .addInputSchemas(stageConfigurer.getInputSchemas());
    Map<String, Schema> outputPortSchemas = stageConfigurer.getOutputPortSchemas();
    for (Map.Entry<String, StageSpec.Port> entry: currentSpec.getOutputPorts().entrySet()) {
      String port = entry.getValue().getPort();
      if (port == null) {
        updatedSpec.addOutput(stageConfigurer.getOutputSchema(), entry.getKey());
      } else {
        updatedSpec.addOutput(entry.getKey(), port, outputPortSchemas.get(port));
      }
    }
    StageSpec updated = updatedSpec.build();
    schemaPropagator.propagateSchema(updated);
    return updated;
  }
}
