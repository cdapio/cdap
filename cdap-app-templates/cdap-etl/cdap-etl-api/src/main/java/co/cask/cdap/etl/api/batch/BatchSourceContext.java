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
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;

import java.util.List;
import java.util.Map;

/**
 * Context of a Batch Source.
 */
@Beta
public interface BatchSourceContext extends BatchContext {

  /**
   * Overrides the input configuration of this Batch job to use the specific stream.
   *
   * @param stream the input stream.
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #setInput(Input)} instead
   */
  @Deprecated
  void setInput(StreamBatchReadable stream);

  /**
   * Overrides the input configuration of this Batch job to use
   * the specified dataset by its name.
   *
   * @param datasetName the name of the input dataset.
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #setInput(Input)} instead
   */
  @Deprecated
  void setInput(String datasetName);

  /**
   * Overrides the input configuration of this Batch job to use
   * the specified dataset by its name and arguments.
   *
   * @param datasetName the the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #setInput(Input)} instead
   */
  @Deprecated
  void setInput(String datasetName, Map<String, String> arguments);

  /**
   * Overrides the input configuration of this Batch job to use
   * the specified dataset by its name and data selection splits.
   *
   * @param datasetName the name of the input dataset
   * @param splits the data selection splits
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #setInput(Input)} instead
   */
  @Deprecated
  void setInput(String datasetName, List<Split> splits);

  /**
   * Overrides the input configuration of this Batch job to use
   * the specified dataset by its name and arguments with the given data selection splits.
   *
   * @param datasetName the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @param splits the data selection splits
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #setInput(Input)} instead
   */
  @Deprecated
  void setInput(String datasetName, Map<String, String> arguments, List<Split> splits);

  /**
   * Overrides the input configuration of this Batch job to the one provided by the given
   * {@link InputFormatProvider}.
   *
   * @param inputFormatProvider provider for InputFormat and configurations to be used
   * @deprecated Deprecated since 3.4.0.
   *             Use {@link #setInput(Input)} instead
   */
  @Deprecated
  void setInput(InputFormatProvider inputFormatProvider);

  /**
   * Overrides the input configuration of this Batch job to the specified {@link Input}.
   *
   * @param input the input to be used
   */
  void setInput(Input input);
}
