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
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;

/**
 * Context of a Batch Sink.
 */
@Beta
public interface BatchSinkContext extends BatchContext {

  /**
   * Overrides the output configuration of this Batch job to write to the specified dataset by its name.
   *
   * @param datasetName the name of the output dataset
   */
  void setOutput(String datasetName);

  /**
   * Overrides the output configuration of this Batch job to write to the specified dataset instance.
   * Currently, the dataset passed in must either be an {@link OutputFormatProvider}.
   * You may want to use this method instead of {@link #setOutput(String)} if your output dataset uses runtime
   * arguments set in your own program logic.
   *
   * @param datasetName the name of the output dataset
   * @param dataset the output dataset
   */
  void setOutput(String datasetName, Dataset dataset);
}
