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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchConfigurable;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.batch.DefaultAggregatorContext;
import io.cdap.cdap.etl.batch.DefaultJoinerContext;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.submit.AggregatorContextProvider;
import io.cdap.cdap.etl.common.submit.ContextProvider;
import io.cdap.cdap.etl.common.submit.Finisher;
import io.cdap.cdap.etl.common.submit.JoinerContextProvider;
import io.cdap.cdap.etl.common.submit.PipelinePhasePreparer;
import io.cdap.cdap.etl.common.submit.SubmitterPlugin;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.batch.BasicSparkPluginContext;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkContext;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkFactory;
import io.cdap.cdap.etl.spark.batch.SparkBatchSourceFactory;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract preparer for spark related pipelines
 */
public abstract class AbstractSparkPreparer extends PipelinePhasePreparer {
  protected Map<String, Integer> stagePartitions;
  protected SparkBatchSourceFactory sourceFactory;
  protected SparkBatchSinkFactory sinkFactory;

  private final Admin admin;
  private final Transactional transactional;

  public AbstractSparkPreparer(PluginContext pluginContext, Metrics metrics,
                               MacroEvaluator macroEvaluator, PipelineRuntime pipelineRuntime,
                               Admin admin, Transactional transactional) {
    super(pluginContext, metrics, macroEvaluator, pipelineRuntime);
    this.admin = admin;
    this.transactional = transactional;
  }

  @Override
  public List<Finisher> prepare(PhaseSpec phaseSpec)
    throws TransactionFailureException, InstantiationException, IOException {
    stageOperations = new HashMap<>();
    stagePartitions = new HashMap<>();
    sourceFactory = new SparkBatchSourceFactory();
    sinkFactory = new SparkBatchSinkFactory();
    return super.prepare(phaseSpec);
  }

  @Nullable
  @Override
  protected SubmitterPlugin create(StageSpec stageSpec, SubmitterLifecycle<?> plugin) throws InstantiationException {
    String stageName = stageSpec.getName();
    if (SparkSink.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
      BatchConfigurable<SparkPluginContext> sparkSink = (BatchConfigurable<SparkPluginContext>) plugin;
      ContextProvider<BasicSparkPluginContext> contextProvider =
        dsContext -> getSparkPluginContext(dsContext, stageSpec);
      return new SubmitterPlugin<>(stageName, transactional, sparkSink, contextProvider,
                                   ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
    }
    if (SparkCompute.PLUGIN_TYPE.equals(stageSpec.getPluginType()) || plugin instanceof SparkCompute) {
      SparkCompute<?, ?> compute = (SparkCompute<?, ?>) plugin;
      ContextProvider<BasicSparkPluginContext> contextProvider =
        dsContext -> getSparkPluginContext(dsContext, stageSpec);
      return new SubmitterPlugin<>(stageName, transactional, compute, contextProvider,
                                   ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
    }
    if (StreamingSource.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
      return createStreamingSource(stageSpec, (StreamingSource<?>) plugin);
    }
    return null;
  }

  @Override
  protected SubmitterPlugin createTransform(Transform<?, ?> transform, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<SparkSubmitterContext> contextProvider =
      dsContext -> getSparkSubmitterContext(dsContext, stageSpec);
    return new SubmitterPlugin<>(stageName, transactional, transform, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createSplitterTransform(SplitterTransform<?, ?> splitterTransform, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<SparkSubmitterContext> contextProvider =
      dsContext -> getSparkSubmitterContext(dsContext, stageSpec);
    return new SubmitterPlugin<>(stageName, transactional, splitterTransform, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createSink(BatchConfigurable<BatchSinkContext> batchSink, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<SparkBatchSinkContext> contextProvider =
      dsContext -> getSparkBatchSinkContext(dsContext, stageSpec);
    return new SubmitterPlugin<>(stageName, transactional, batchSink, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createAggregator(BatchAggregator<?, ?, ?> aggregator, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultAggregatorContext> contextProvider =
      new AggregatorContextProvider(pipelineRuntime, stageSpec, admin);
    return new SubmitterPlugin<>(stageName, transactional, aggregator, contextProvider,
                                 ctx -> {
                                   stagePartitions.put(stageName, ctx.getNumPartitions());
                                   stageOperations.put(stageName, ctx.getFieldOperations());
                                 });
  }

  @Override
  protected SubmitterPlugin createReducibleAggregator(BatchReducibleAggregator<?, ?, ?, ?> aggregator,
                                                      StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultAggregatorContext> contextProvider =
      new AggregatorContextProvider(pipelineRuntime, stageSpec, admin);
    return new SubmitterPlugin<>(stageName, transactional, aggregator, contextProvider,
                                 ctx -> {
                                   stagePartitions.put(stageName, ctx.getNumPartitions());
                                   stageOperations.put(stageName, ctx.getFieldOperations());
                                 });
  }

  @Override
  protected SubmitterPlugin createJoiner(BatchJoiner<?, ?, ?> batchJoiner, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultJoinerContext> contextProvider =
      new JoinerContextProvider(pipelineRuntime, stageSpec, admin);
    return new SubmitterPlugin<>(stageName, transactional, batchJoiner, contextProvider, sparkJoinerContext -> {
      stagePartitions.put(stageName, sparkJoinerContext.getNumPartitions());
      stageOperations.put(stageName, sparkJoinerContext.getFieldOperations());
    });
  }

  @Override
  protected SubmitterPlugin createAutoJoiner(BatchAutoJoiner batchJoiner, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultJoinerContext> contextProvider =
      new JoinerContextProvider(pipelineRuntime, stageSpec, admin);
    return new SubmitterPlugin<>(stageName, transactional, batchJoiner, contextProvider, sparkJoinerContext -> {
      stagePartitions.put(stageName, sparkJoinerContext.getNumPartitions());
      stageOperations.put(stageName, sparkJoinerContext.getFieldOperations());
    });
  }

  protected abstract SparkBatchSinkContext getSparkBatchSinkContext(DatasetContext dsContext, StageSpec stageSpec);

  protected abstract BasicSparkPluginContext getSparkPluginContext(DatasetContext dsContext, StageSpec stageSpec);

  protected abstract SparkSubmitterContext getSparkSubmitterContext(DatasetContext dsContext, StageSpec stageSpec);

  // only streaming pipeline can create streaming source
  @Nullable
  protected SubmitterPlugin createStreamingSource(StageSpec stageSpec, StreamingSource<?> streamingSource)
    throws InstantiationException {

    return null;
  }
}
