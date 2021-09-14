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

package io.cdap.cdap.etl.spark.streaming;

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.BatchConfigurable;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.submit.ContextProvider;
import io.cdap.cdap.etl.common.submit.SubmitterPlugin;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.AbstractSparkPreparer;
import io.cdap.cdap.etl.spark.SparkSubmitterContext;
import io.cdap.cdap.etl.spark.batch.BasicSparkPluginContext;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkContext;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Preparer for spark streaming pipeline
 */
public class SparkStreamingPreparer extends AbstractSparkPreparer {
  private final JavaSparkExecutionContext context;

  public SparkStreamingPreparer(PluginContext pluginContext, Metrics metrics, MacroEvaluator macroEvaluator,
                                PipelineRuntime pipelineRuntime, JavaSparkExecutionContext context) {
    super(pluginContext, metrics, macroEvaluator, pipelineRuntime, context.getAdmin(), context);
    this.context = context;
  }

  public Set<String> getUncombinableSinks() {
    return sinkFactory.getUncombinableSinks();
  }

  @Nullable
  @Override
  protected SubmitterPlugin createSource(BatchConfigurable<BatchSourceContext> batchSource, StageSpec stageSpec) {
    return null;
  }

  @Override
  protected SubmitterPlugin createStreamingSource(StageSpec stageSpec, StreamingSource<?> streamingSource)
    throws InstantiationException {

    String stageName = stageSpec.getName();
    ContextProvider<DefaultStreamingSourceContext> contextProvider =
      dsContext -> new DefaultStreamingSourceContext(pipelineRuntime, stageSpec, dsContext, context);
    return new SubmitterPlugin<>(stageName, context, streamingSource, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SparkBatchSinkContext getSparkBatchSinkContext(DatasetContext dsContext, StageSpec stageSpec) {
    return new SparkBatchSinkContext(sinkFactory, context, dsContext, pipelineRuntime, stageSpec);
  }

  @Override
  protected BasicSparkPluginContext getSparkPluginContext(DatasetContext dsContext, StageSpec stageSpec) {
    return new BasicSparkPluginContext(null, pipelineRuntime, stageSpec, dsContext, context.getAdmin());
  }

  @Override
  protected SparkSubmitterContext getSparkSubmitterContext(DatasetContext dsContext, StageSpec stageSpec) {
    return new SparkSubmitterContext(context, pipelineRuntime, dsContext, stageSpec);
  }

  public Map<String, List<FieldOperation>> getFieldOperations() {
    return stageOperations;
  }
}
