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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.ExternalDatasets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link BatchSourceContext} for spark contexts.
 */
public class SparkBatchSourceContext extends AbstractSparkBatchContext implements BatchSourceContext {

  private SparkBatchSourceFactory sourceFactory;

  public SparkBatchSourceContext(SparkClientContext sparkContext, LookupProvider lookupProvider, String stageId) {
    super(sparkContext, lookupProvider, stageId);
  }

  @Override
  public void setInput(StreamBatchReadable stream) {
    sourceFactory = SparkBatchSourceFactory.create(stream);
  }

  @Override
  public void setInput(String datasetName) {
    sourceFactory = SparkBatchSourceFactory.create(datasetName);
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments) {
    sourceFactory = SparkBatchSourceFactory.create(datasetName, arguments);
  }

  @Override
  public void setInput(String datasetName, List<Split> splits) {
    sourceFactory = SparkBatchSourceFactory.create(datasetName, Collections.<String, String>emptyMap(), splits);
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments, List<Split> splits) {
    sourceFactory = SparkBatchSourceFactory.create(datasetName, arguments, splits);
  }

  @Override
  public void setInput(InputFormatProvider inputFormatProvider) {
    sourceFactory = SparkBatchSourceFactory.create(inputFormatProvider);
  }

  @Override
  public void setInput(String datasetName, Dataset dataset) {
    if (dataset instanceof BatchReadable) {
      setInput(datasetName, ((BatchReadable) dataset).getSplits());
    } else if (dataset instanceof InputFormatProvider) {
      setInput((InputFormatProvider) dataset);
    } else {
      throw new IllegalArgumentException("Input dataset must be a BatchReadable or InputFormatProvider.");
    }
  }

  @Override
  public void setInput(Input input) {
    Input trackableInput = ExternalDatasets.makeTrackable(sparkContext.getAdmin(), input);
    sourceFactory = SparkBatchSourceFactory.create(trackableInput);
  }

  @Nullable
  SparkBatchSourceFactory getSourceFactory() {
    return sourceFactory;
  }
}
