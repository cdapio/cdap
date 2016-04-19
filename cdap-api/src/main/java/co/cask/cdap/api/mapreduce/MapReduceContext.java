/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.api.mapreduce;

import co.cask.cdap.api.ClientLocalizationContext;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.workflow.WorkflowInfoProvider;

import java.util.List;
import java.util.Map;

/**
 * MapReduce job execution context.
 */
public interface MapReduceContext extends RuntimeContext, DatasetContext, ServiceDiscoverer,
                                          PluginContext, ClientLocalizationContext, WorkflowInfoProvider {

  /**
   * @return The specification used to configure this {@link MapReduce} job instance.
   */
  MapReduceSpecification getSpecification();

  /**
   * Returns the logical start time of this MapReduce job. Logical start time is the time when this MapReduce
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   */
  <T> T getHadoopJob();

  /**
   * Overrides the input configuration of this MapReduce job to use the specific stream as the single input.
   *
   * @param stream the input stream.
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addInput(Input)} instead.
   */
  @Deprecated
  void setInput(StreamBatchReadable stream);

  /**
   * Overrides the input configuration of this MapReduce job to use the specified dataset by its name,
   * as the single input.
   *
   * @param datasetName the name of the input dataset.
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addInput(Input)} instead.
   */
  @Deprecated
  void setInput(String datasetName);

  /**
   * Overrides the input configuration of this MapReduce job to use he specified dataset by its name and arguments,
   * as the single input.
   *
   * @param datasetName the the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addInput(Input)} instead.
   */
  @Deprecated
  void setInput(String datasetName, Map<String, String> arguments);

  /**
   * Overrides the input configuration of this MapReduce job to use the specified dataset by its name and data
   * selection splits, as teh single input
   *
   * @param datasetName the name of the input dataset
   * @param splits the data selection splits
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addInput(Input)} instead.
   */
  @Deprecated
  void setInput(String datasetName, List<Split> splits);

  /**
   * Overrides the input configuration of this MapReduce job to use the specified dataset by its name and arguments
   * with the given data selection splits, as the single input.
   *
   * @param datasetName the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @param splits the data selection splits
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addInput(Input)} instead.
   */
  @Deprecated
  void setInput(String datasetName, Map<String, String> arguments, List<Split> splits);

  /**
   * Overrides the input configuration of this MapReduce job to the one provided by the given
   * {@link InputFormatProvider}, as the single input.
   *
   * @param inputFormatProvider provider for InputFormat and configurations to be used
   * @deprecated as of 3.4.0. Use {@link MapReduceContext#addInput(Input)} instead.
   */
  @Deprecated
  void setInput(InputFormatProvider inputFormatProvider);

  /**
   * Overrides the input configuration of this MapReduce job to read from the specified dataset instance,
   * as the single input.
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
   * @deprecated as of version 3.3.0. Use {@link #addInput(Input)} instead.
   */
  @Deprecated
  void setInput(String datasetName, Dataset dataset);

  /**
   * Updates the input configuration of this MapReduce job to use the specified {@link Input}.
   * @param input the input to be used
   * @throws IllegalStateException if called after any setInput methods.
   */
  void addInput(Input input);

  /**
   * Updates the input configuration of this MapReduce job to use the specified {@link Input}.
   * @param input the input to be used
   * @param mapperCls the mapper class to be used for the input
   * @throws IllegalStateException if called after any setInput methods.
   */
  void addInput(Input input, Class<?> mapperCls);

  /**
   * Overrides the output configuration of this MapReduce job to write to the specified dataset by its name.
   *
   * @param datasetName the name of the output dataset
   * Deprecated as of version 3.2.0. Use {@link #addOutput(Output)}, instead.
   */
  @Deprecated
  void setOutput(String datasetName);

  /**
   * Overrides the output configuration of this MapReduce job to write to the specified dataset instance.
   * Currently, the dataset passed in must be an {@link OutputFormatProvider}.
   * You may want to use this method instead of {@link #setOutput(String)} if your output dataset uses runtime
   * arguments set in your own program logic.
   *
   * @param datasetName the name of the output dataset
   * @param dataset the output dataset
   * @throws IllegalArgumentException if the given dataset is not an OutputFormatProvider.
   * Deprecated as of version 3.2.0. Use {@link #addOutput(Output)}, instead.
   */
  @Deprecated
  void setOutput(String datasetName, Dataset dataset);

  /**
   * Overrides the output configuration of this MapReduce job to also allow writing to the specified dataset by
   * its name.
   *
   * @param datasetName the name of the output dataset
   * Deprecated as of version 3.4.0. Use {@link #addOutput(Output)}, instead.
   */
  @Deprecated
  void addOutput(String datasetName);

  /**
   * Updates the output configuration of this MapReduce job to also allow writing to the specified dataset.
   * Currently, the dataset specified in must be an {@link OutputFormatProvider}.
   * You may want to use this method instead of {@link #addOutput(String)} if your output dataset uses runtime
   * arguments set in your own program logic.
   *
   * @param datasetName the name of the output dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @throws IllegalArgumentException if the specified dataset is not an OutputFormatProvider.
   * Deprecated as of version 3.4.0. Use {@link #addOutput(Output)}, instead.
   */
  @Deprecated
  void addOutput(String datasetName, Map<String, String> arguments);

  /**
   * Updates the output configuration of this MapReduce job to also allow writing using the given
   * {@link OutputFormatProvider}.
   *
   * @param alias the alias of the output
   * @param outputFormatProvider the outputFormatProvider which specifies an OutputFormat and configuration to be used
   *                             when writing to this output
   * Deprecated as of version 3.4.0. Use {@link #addOutput(Output)}, instead.
   */
  @Deprecated
  void addOutput(String alias, OutputFormatProvider outputFormatProvider);

  // TODO: (CDAP-5651) change usages of above three deprecated methods to use this
  /**
   * Updates the output configuration of this MapReduce job to use the specified {@link Output}.
   * @param output the output to be used
   */
  void addOutput(Output output);

  /**
   * Overrides the resources, such as memory and virtual cores, to use for each mapper of this MapReduce job.
   *
   * @param resources Resources that each mapper should use.
   */
  void setMapperResources(Resources resources);

  /**
   * Override the resources, such as memory and virtual cores, to use for each reducer of this MapReduce job.
   *
   * @param resources Resources that each reducer should use.
   */
  void setReducerResources(Resources resources);
}
