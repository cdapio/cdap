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

package co.cask.cdap.template.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;

import java.util.List;

/**
 * Context of a Batch Source.
 */
@Beta
public interface BatchSourceContext extends BatchContext {

  /**
   * Overrides the input configuration of this Batch job to use the specific stream.
   *
   * @param stream the input stream.
   */
  void setInput(StreamBatchReadable stream);

  /**
   * Overrides the input configuration of this Batch job to use
   * the specified dataset by its name.
   *
   * @param datasetName the name of the input dataset.
   */
  void setInput(String datasetName);

  /**
   * Overrides the input configuration of this Batch job to use
   * the specified dataset by its name and data selection splits.
   *
   * @param datasetName the name of the input dataset
   * @param splits the data selection splits
   */
  void setInput(String datasetName, List<Split> splits);

  /**
   * Overrides the input configuration of this MapReduce job to write to the specified dataset instance.
   *
   * <p>
   * Currently, the dataset passed in must either be an {@link InputFormatProvider} or a {@link BatchReadable}.
   * You may want to use this method instead of {@link #setInput(String, List)} if your input dataset uses runtime
   * arguments set in your own program logic. Input splits are determined either from the dataset instance if it is
   * a {@link BatchReadable} or from the corresponding MapReduce InputFormat if the dataset instance is a
   * {@link InputFormatProvider}.
   * </p>
   *
   * @param datasetName the name of the input dataset
   * @param dataset the input dataset
   */
  void setInput(String datasetName, Dataset dataset);
}
