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
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProviderSpec;
import io.cdap.cdap.etl.batch.BasicInputFormatProvider;
import io.cdap.cdap.etl.batch.preview.LimitingInputFormatProvider;
import io.cdap.cdap.etl.common.ErrorDetails;
import io.cdap.cdap.etl.common.ExternalDatasets;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkSubmitterContext;
import io.cdap.cdap.etl.spark.io.StageTrackingInputFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Default implementation of {@link BatchSourceContext} for spark contexts.
 */
public class SparkBatchSourceContext extends SparkSubmitterContext implements BatchSourceContext {

  private final SparkBatchSourceFactory sourceFactory;
  private final boolean isPreviewEnabled;
  private ErrorDetailsProviderSpec errorDetailsProviderSpec;

  public SparkBatchSourceContext(SparkBatchSourceFactory sourceFactory,
      SparkClientContext sparkContext, PipelineRuntime pipelineRuntime,
      DatasetContext datasetContext, StageSpec stageSpec) {
    super(sparkContext, pipelineRuntime, datasetContext, StageSpec.
        createCopy(stageSpec, sparkContext.getDataTracer(
            stageSpec.getName()).getMaximumTracedRecords(),
            sparkContext.getDataTracer(stageSpec.getName()).isEnabled()));
    this.sourceFactory = sourceFactory;
    this.isPreviewEnabled = stageSpec.isPreviewEnabled(sparkContext);
  }

  @Override
  public void setErrorDetailsProvider(ErrorDetailsProviderSpec errorDetailsProviderSpec) {
    this.errorDetailsProviderSpec = errorDetailsProviderSpec;
  }

  @Override
  public void setInput(Input input) {
    Input trackableInput = input;

    // Wrap the input provider with tracking counter for metrics collection via MR counter.
    if (trackableInput instanceof Input.InputFormatProviderInput) {
      InputFormatProvider provider = ((Input.InputFormatProviderInput) trackableInput).getInputFormatProvider();
      Map<String, String> conf = new HashMap<>(provider.getInputFormatConfiguration());
      conf.put(StageTrackingInputFormat.DELEGATE_CLASS_NAME, provider.getInputFormatClassName());
      conf.put(StageTrackingInputFormat.WRAPPED_STAGE_NAME, getStageName());
      if (errorDetailsProviderSpec != null) {
        conf.put(ErrorDetails.ERROR_DETAILS_PROVIDER_CLASS_NAME_KEY,
            errorDetailsProviderSpec.getClassName());
      }
      provider = new BasicInputFormatProvider(StageTrackingInputFormat.class.getName(), conf);
      trackableInput = Input.of(trackableInput.getName(), provider).alias(trackableInput.getAlias());
    }

    // Limit preview input by wrapping the input
    if (isPreviewEnabled && trackableInput instanceof Input.InputFormatProviderInput) {
      InputFormatProvider inputFormatProvider =
        ((Input.InputFormatProviderInput) trackableInput).getInputFormatProvider();
      LimitingInputFormatProvider wrapper =
        new LimitingInputFormatProvider(inputFormatProvider, getMaxPreviewRecords());
      trackableInput = Input.of(trackableInput.getName(), wrapper).alias(trackableInput.getAlias());
    }
    trackableInput = ExternalDatasets.makeTrackable(admin, suffixInput(trackableInput));
    sourceFactory.addInput(getStageName(), trackableInput);
  }

  @Override
  public boolean isPreviewEnabled() {
    return isPreviewEnabled;
  }

  private Input suffixInput(Input input) {
    String suffixedAlias = String.format("%s-%s", input.getAlias(), UUID.randomUUID());
    return input.alias(suffixedAlias);
  }
}
