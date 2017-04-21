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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;

import java.util.Map;

/**
 * Context of a Batch Sink.
 */
@Beta
public interface BatchSinkContext extends BatchContext {

  /**
   * Overrides the output configuration of this job to also allow writing to the specified dataset by
   * its name.
   *
   * @param datasetName the name of the output dataset
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #addOutput(Output)} instead
   */
  @Deprecated
  void addOutput(String datasetName);

  /**
   * Updates the output configuration of this job to also allow writing to the specified dataset.
   * Currently, the dataset specified in must be an {@link OutputFormatProvider}.
   * You may want to use this method instead of {@link #addOutput(String)} if your output dataset uses runtime
   * arguments set in your own program logic.
   *
   * @param datasetName the name of the output dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @throws IllegalArgumentException if the specified dataset is not an OutputFormatProvider.
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #addOutput(Output)} instead
   */
  @Deprecated
  void addOutput(String datasetName, Map<String, String> arguments);

  /**
   * Updates the output configuration of this job to also allow writing using the given OutputFormatProvider.
   *
   * @param outputName the name of the output
   * @param outputFormatProvider the outputFormatProvider which specifies an OutputFormat and configuration to be used
   *                             when writing to this output
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #addOutput(Output)} instead
   */
  @Deprecated
  void addOutput(String outputName, OutputFormatProvider outputFormatProvider);

  /**
   * Updates the output configuration of this job to also allow writing using the given output.
   *
   * @param output output to be used
   */
  void addOutput(Output output);

  /**
   * Indicates whether the pipeline is running in preview.
   *
   * @return a boolean value which indicates the pipeline is running in preview mode.
   */
  boolean isPreviewEnabled();
}
