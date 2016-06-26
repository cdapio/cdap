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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.ExternalDatasets;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link BatchSourceContext} for spark contexts.
 */
public class SparkBatchSourceContext extends AbstractSparkBatchContext implements BatchSourceContext {

  private final String aliasSuffix;
  private SparkBatchSourceFactory sourceFactory;
  private Input externalInput;

  public SparkBatchSourceContext(SparkClientContext sparkContext, LookupProvider lookupProvider, String stageId,
                                 String aliasSuffix) {
    super(sparkContext, lookupProvider, stageId);
    this.aliasSuffix = aliasSuffix;
  }

  @Override
  public void setInput(StreamBatchReadable stream) {
    FormatSpecification formatSpec = stream.getFormatSpecification();
    Input streamInput;
    if (formatSpec == null) {
      streamInput = Input.ofStream(stream.getStreamName(), stream.getStartTime(), stream.getEndTime());
    } else {
      streamInput = Input.ofStream(stream.getStreamName(), stream.getStartTime(), stream.getEndTime(), formatSpec);
    }
    externalInput = suffixInput(streamInput);
    sourceFactory = SparkBatchSourceFactory.create(externalInput, getStageName());
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
    externalInput = suffixInput(Input.ofDataset(datasetName, Collections.<String, String>emptyMap(), splits));
    sourceFactory = SparkBatchSourceFactory.create(externalInput, getStageName());
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments, List<Split> splits) {
    externalInput = suffixInput(Input.ofDataset(datasetName, arguments, splits));
    sourceFactory = SparkBatchSourceFactory.create(externalInput, getStageName());
  }

  @Override
  public void setInput(InputFormatProvider inputFormatProvider) {
    externalInput = suffixInput(Input.of(inputFormatProvider.getInputFormatClassName(), inputFormatProvider));
    sourceFactory = SparkBatchSourceFactory.create(externalInput, getStageName());
  }

  @Override
  public void setInput(Input input) {
    externalInput = suffixInput(input);
    Input trackableInput = ExternalDatasets.makeTrackable(sparkContext.getAdmin(), externalInput);
    sourceFactory = SparkBatchSourceFactory.create(trackableInput, getStageName());
  }

  @Nullable
  SparkBatchSourceFactory getSourceFactory() {
    return sourceFactory;
  }

  public String getAlias() {
    return externalInput.getAlias();
  }

  private Input suffixInput(Input input) {
    if (Strings.isNullOrEmpty(aliasSuffix)) {
      return input;
    }

    String suffixedAlias = (input.getAlias() != null) ?
      String.format("%s_%s", input.getAlias(), aliasSuffix) : aliasSuffix;
    return input.alias(suffixedAlias);
  }
}
