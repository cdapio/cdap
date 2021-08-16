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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.batch.BatchConfigurable;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.FieldOperationTypeAdapter;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.SetMultimapCodec;
import io.cdap.cdap.etl.common.submit.ContextProvider;
import io.cdap.cdap.etl.common.submit.Finisher;
import io.cdap.cdap.etl.common.submit.SubmitterPlugin;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.AbstractSparkPreparer;
import io.cdap.cdap.etl.spark.SparkSubmitterContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
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

/**
 * Prepares Spark batch jobs.
 */
public class SparkPreparer extends AbstractSparkPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPreparer.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
    .registerTypeAdapter(InputFormatProvider.class, new InputFormatProviderTypeAdapter())
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter())
    .create();
  private final SparkClientContext context;

  public SparkPreparer(SparkClientContext context,
                       Metrics metrics,
                       MacroEvaluator macroEvaluator,
                       PipelineRuntime pipelineRuntime) {
    super(context, metrics, macroEvaluator, pipelineRuntime, context.getAdmin(), context);
    this.context = context;
  }

  @Override
  public List<Finisher> prepare(PhaseSpec phaseSpec)
    throws TransactionFailureException, InstantiationException, IOException {
    stageOperations = new HashMap<>();
    stagePartitions = new HashMap<>();
    // TODO: Make sure working directory is present
    File configFile = new File("HydratorSpark.config");
    //File configFile = File.createTempFile("HydratorSpark", ".config");

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

  @Override
  protected SubmitterPlugin createSource(BatchConfigurable<BatchSourceContext> batchSource, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<SparkBatchSourceContext> contextProvider =
      dsContext -> new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, dsContext, stageSpec);
    return new SubmitterPlugin<>(stageName, context, batchSource, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SparkBatchSinkContext getSparkBatchSinkContext(DatasetContext dsContext, StageSpec stageSpec) {
    return new SparkBatchSinkContext(sinkFactory, context, pipelineRuntime, dsContext, stageSpec);
  }

  @Override
  protected BasicSparkPluginContext getSparkPluginContext(DatasetContext dsContext, StageSpec stageSpec) {
    return new BasicSparkPluginContext(context, pipelineRuntime, stageSpec, dsContext, context.getAdmin());
  }

  @Override
  protected SparkSubmitterContext getSparkSubmitterContext(DatasetContext dsContext, StageSpec stageSpec) {
    return new SparkSubmitterContext(context, pipelineRuntime, dsContext, stageSpec);
  }
}
