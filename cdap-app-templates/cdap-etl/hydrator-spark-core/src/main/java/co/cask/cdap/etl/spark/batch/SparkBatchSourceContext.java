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

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.ExternalDatasets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Default implementation of {@link BatchSourceContext} for spark contexts.
 */
public class SparkBatchSourceContext extends AbstractSparkBatchContext implements BatchSourceContext {
  private final SparkBatchSourceFactory sourceFactory;

  public SparkBatchSourceContext(SparkBatchSourceFactory sourceFactory, SparkClientContext sparkContext,
                                 LookupProvider lookupProvider, String stageId) {
    super(sparkContext, lookupProvider, stageId);
    this.sourceFactory = sourceFactory;
  }

  @Override
  public void setInput(StreamBatchReadable stream) {
    FormatSpecification formatSpec = stream.getFormatSpecification();
    Input input;
    if (formatSpec == null) {
      input = suffixInput(Input.ofStream(stream.getStreamName(), stream.getStartTime(), stream.getEndTime()));
    } else {
      input = suffixInput(Input.ofStream(stream.getStreamName(), stream.getStartTime(),
                                         stream.getEndTime(), formatSpec));
    }
    sourceFactory.addInput(getStageName(), input);
  }

  @Override
  public void setInput(String datasetName) {
    setInput(datasetName, Collections.<String, String>emptyMap());
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments) {
    setInput(datasetName, arguments, null);
  }

  @Override
  public void setInput(String datasetName, List<Split> splits) {
    setInput(datasetName, Collections.<String, String>emptyMap(), splits);
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments, List<Split> splits) {
    sourceFactory.addInput(getStageName(), suffixInput(Input.ofDataset(datasetName, arguments, splits)));
  }

  @Override
  public void setInput(InputFormatProvider inputFormatProvider) {
    sourceFactory.addInput(getStageName(), suffixInput(Input.of(inputFormatProvider.getInputFormatClassName(),
                                                                inputFormatProvider)));
  }

  @Override
  public void setInput(Input input) {
    Input trackableInput = ExternalDatasets.makeTrackable(admin, suffixInput(input));
    sourceFactory.addInput(getStageName(), trackableInput);
  }

  private Input suffixInput(Input input) {
    String suffixedAlias = String.format("%s-%s", input.getAlias(), UUID.randomUUID());
    return input.alias(suffixedAlias);
  }
}
