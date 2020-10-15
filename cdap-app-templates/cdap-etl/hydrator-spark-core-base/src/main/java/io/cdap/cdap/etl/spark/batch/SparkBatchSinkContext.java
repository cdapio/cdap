/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.batch.AbstractBatchContext;
import io.cdap.cdap.etl.batch.BasicOutputFormatProvider;
import io.cdap.cdap.etl.batch.preview.NullOutputFormatProvider;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.io.TrackingOutputFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Default implementation of {@link BatchSinkContext} for spark contexts.
 */
public class SparkBatchSinkContext extends AbstractBatchContext implements BatchSinkContext {
  private final SparkBatchSinkFactory sinkFactory;
  private final boolean isPreviewEnabled;

  public SparkBatchSinkContext(SparkBatchSinkFactory sinkFactory, SparkClientContext sparkContext,
                               PipelineRuntime pipelineRuntime, DatasetContext datasetContext, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, datasetContext, sparkContext.getAdmin());
    this.sinkFactory = sinkFactory;
    this.isPreviewEnabled = sparkContext.getDataTracer(stageSpec.getName()).isEnabled();
  }

  public SparkBatchSinkContext(SparkBatchSinkFactory sinkFactory, JavaSparkExecutionContext sec,
                               DatasetContext datasetContext, PipelineRuntime pipelineRuntime, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, datasetContext, sec.getAdmin());
    this.sinkFactory = sinkFactory;
    this.isPreviewEnabled = sec.getDataTracer(stageSpec.getName()).isEnabled();
  }

  @Override
  public void addOutput(Output output) {
    Output actualOutput = suffixOutput(getOutput(output));

    // Wrap the output provider with tracking counter for metrics collection via MR counter.
    if (actualOutput instanceof Output.OutputFormatProviderOutput) {
      OutputFormatProvider provider = ((Output.OutputFormatProviderOutput) actualOutput).getOutputFormatProvider();
      Map<String, String> conf = new HashMap<>(provider.getOutputFormatConfiguration());
      conf.put(TrackingOutputFormat.DELEGATE_CLASS_NAME, provider.getOutputFormatClassName());
      provider = new BasicOutputFormatProvider(TrackingOutputFormat.class.getName(), conf);
      actualOutput = Output.of(actualOutput.getName(), provider).alias(actualOutput.getAlias());
    }

    sinkFactory.addOutput(getStageName(), actualOutput);
  }

  @Override
  public boolean isPreviewEnabled() {
    return isPreviewEnabled;
  }

  /**
   * Suffix the alias of {@link Output} so that aliases of outputs are unique.
   */
  private Output suffixOutput(Output output) {
    String suffixedAlias = String.format("%s-%s", output.getAlias(), UUID.randomUUID());
    return output.alias(suffixedAlias);
  }

  /**
   * Get the output, if preview is enabled, return the output with a {@link NullOutputFormatProvider}.
   */
  private Output getOutput(Output output) {
    if (isPreviewEnabled) {
      return Output.of(output.getName(), new NullOutputFormatProvider());
    }
    return output;
  }
}
