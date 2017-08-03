/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.batch.AbstractBatchContext;
import co.cask.cdap.etl.batch.preview.NullOutputFormatProvider;
import co.cask.cdap.etl.common.ExternalDatasets;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.spec.StageSpec;

import java.util.Collections;
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
  public void addOutput(String datasetName) {
    addOutput(datasetName, Collections.<String, String>emptyMap());
  }

  @Override
  public void addOutput(String datasetName, Map<String, String> arguments) {
    sinkFactory.addOutput(getStageName(), suffixOutput(getOutput(Output.ofDataset(datasetName, arguments))));
  }

  @Override
  public void addOutput(String outputName, OutputFormatProvider outputFormatProvider) {
    sinkFactory.addOutput(getStageName(), suffixOutput(getOutput(Output.of(outputName, outputFormatProvider))));
  }

  @Override
  public void addOutput(Output output) {
    Output actualOutput = suffixOutput(getOutput(output));
    Output trackableOutput = isPreviewEnabled ? actualOutput : ExternalDatasets.makeTrackable(admin, actualOutput);
    sinkFactory.addOutput(getStageName(), trackableOutput);
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
