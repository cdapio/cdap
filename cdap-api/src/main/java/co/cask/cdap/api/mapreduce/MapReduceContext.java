/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;

import java.util.List;

/**
 * MapReduce job execution context.
 */
public interface MapReduceContext extends RuntimeContext, DatasetContext, ServiceDiscoverer {
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
   * Overrides the input configuration of this MapReduce job to use
   * the specified dataset by its name.
   *
   * @param datasetName Name of the input dataset.
   */
  void setInput(String datasetName);

  /**
   * Overrides the input configuration of this MapReduce job to use
   * the specified dataset by its name and data selection splits.
   *
   * @param datasetName Name of the input dataset.
   * @param splits Data selection splits.
   */
  void setInput(String datasetName, List<Split> splits);

  /**
   * Overrides the input configuration of this MapReduce job to write to the specified dataset instance.
   * Currently, the dataset passed in must either be an {@link InputFormatProvider} or a {@link BatchReadable}.
   * You may want to use this instead of {@link #setInput(String, List)} if your input dataset uses runtime
   * arguments set in your own program logic. Input splits are determined from the dataset instance if it is
   * a {@link BatchReadable}, or from the corresponding mapreduce InputFormat if the dataset instance is a
   * {@link InputFormatProvider}.
   *
   * @param datasetName Name of the input dataset.
   * @param dataset Input dataset.
   */
  void setInput(String datasetName, Dataset dataset);

  /**
   * Overrides the output configuration of this MapReduce job to write to the specified dataset by its name.
   *
   * @param datasetName Name of the output dataset.
   */
  void setOutput(String datasetName);

  /**
   * Overrides the output configuration of this MapReduce job to write to the specified dataset instance.
   * Currently, the dataset passed in must either be an {@link OutputFormatProvider}.
   * You may want to use this instead of {@link #setOutput(String)} if your output dataset uses runtime
   * arguments set in your own program logic.
   *
   * @param datasetName Name of the output dataset.
   * @param dataset Output dataset.
   */
  void setOutput(String datasetName, Dataset dataset);
}
