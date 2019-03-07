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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.DefaultAggregatorContext;
import co.cask.cdap.etl.batch.DefaultJoinerContext;
import co.cask.cdap.etl.batch.PipelinePhasePreparer;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.FieldOperationTypeAdapter;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.submit.AggregatorContextProvider;
import co.cask.cdap.etl.common.submit.ContextProvider;
import co.cask.cdap.etl.common.submit.Finisher;
import co.cask.cdap.etl.common.submit.JoinerContextProvider;
import co.cask.cdap.etl.common.submit.SubmitterPlugin;
import co.cask.cdap.etl.proto.v2.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Prepares Spark jobs.
 */
public class SparkPreparer extends PipelinePhasePreparer {
  private static final Logger LOG = LoggerFactory.getLogger(SparkPreparer.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
    .registerTypeAdapter(OutputFormatProvider.class, new OutputFormatProviderTypeAdapter())
    .registerTypeAdapter(InputFormatProvider.class, new InputFormatProviderTypeAdapter())
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter())
    .create();
  private final SparkClientContext context;
  private Map<String, List<FieldOperation>> stageOperations;
  private SparkBatchSourceFactory sourceFactory;
  private SparkBatchSinkFactory sinkFactory;
  private Map<String, Integer> stagePartitions;

  public SparkPreparer(SparkClientContext context,
                       Metrics metrics,
                       MacroEvaluator macroEvaluator,
                       PipelineRuntime pipelineRuntime) {
    super(context, metrics, macroEvaluator, pipelineRuntime);
    this.context = context;
  }

  @Override
  public List<Finisher> prepare(BatchPhaseSpec phaseSpec)
    throws TransactionFailureException, InstantiationException, IOException {
    stageOperations = new HashMap<>();
    stagePartitions = new HashMap<>();
    sourceFactory = new SparkBatchSourceFactory();
    sinkFactory = new SparkBatchSinkFactory();
    File configFile = File.createTempFile("HydratorSpark", ".config");

    List<Finisher> finishers = super.prepare(phaseSpec);
    finishers.add(new Finisher() {
      @Override
      public void onFinish(boolean succeeded) {
        if (!configFile.delete()) {
          LOG.warn("Failed to clean up resource {} ", configFile);
        }
      }
    });

    try (Writer writer = Files.newBufferedWriter(configFile.toPath(), StandardCharsets.UTF_8)) {
      SparkBatchSourceSinkFactoryInfo sourceSinkInfo = new SparkBatchSourceSinkFactoryInfo(sourceFactory,
                                                                                           sinkFactory,
                                                                                           stagePartitions);
      writer.write(GSON.toJson(sourceSinkInfo));
    }

    context.localize("HydratorSpark.config", configFile.toURI());
    WorkflowToken token = context.getWorkflowToken();
    if (token != null) {
      for (Map.Entry<String, String> entry : pipelineRuntime.getArguments().getAddedArguments().entrySet()) {
        token.put(entry.getKey(), entry.getValue());
      }
      // Put the collected field operations in workflow token
      token.put(Constants.FIELD_OPERATION_KEY_IN_WORKFLOW_TOKEN, GSON.toJson(stageOperations));
    }
    return finishers;
  }

  @Nullable
  @Override
  protected SubmitterPlugin create(PipelinePluginInstantiator pluginInstantiator,
                                   StageSpec stageSpec) throws InstantiationException {
    String stageName = stageSpec.getName();
    if (SparkSink.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
      BatchConfigurable<SparkPluginContext> sparkSink = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
      ContextProvider<BasicSparkPluginContext> contextProvider =
        dsContext -> new BasicSparkPluginContext(context, pipelineRuntime, stageSpec, dsContext, context.getAdmin());
      return new SubmitterPlugin<>(stageName, context, sparkSink, contextProvider);
    }
    return null;
  }

  @Override
  protected SubmitterPlugin createSource(BatchConfigurable<BatchSourceContext> batchSource, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<SparkBatchSourceContext> contextProvider =
      dsContext -> new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, dsContext, stageSpec);
    return new SubmitterPlugin<>(stageName, context, batchSource, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createSink(BatchConfigurable<BatchSinkContext> batchSink, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<SparkBatchSinkContext> contextProvider =
      dsContext -> new SparkBatchSinkContext(sinkFactory, context, pipelineRuntime, dsContext, stageSpec);
    return new SubmitterPlugin<>(stageName, context, batchSink, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createTransform(Transform<?, ?> transform, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<SparkBatchSourceContext> contextProvider =
      dsContext -> new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, dsContext, stageSpec);
    return new SubmitterPlugin<>(stageName, context, transform, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createAggregator(BatchAggregator<?, ?, ?> aggregator, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultAggregatorContext> contextProvider =
      new AggregatorContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
    return new SubmitterPlugin<>(stageName, context, aggregator, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createJoiner(BatchJoiner<?, ?, ?> batchJoiner, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultJoinerContext> contextProvider =
      new JoinerContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
    return new SubmitterPlugin<>(stageName, context, batchJoiner, contextProvider, sparkJoinerContext -> {
      stagePartitions.put(stageName, sparkJoinerContext.getNumPartitions());
      stageOperations.put(stageName, sparkJoinerContext.getFieldOperations());
    });
  }
}
