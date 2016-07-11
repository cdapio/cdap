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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.etl.api.TransformContext;

import java.util.Map;

/**
 * Context passed to Batch Source and Sink.
 */
@Beta
public interface BatchContext extends DatasetContext, TransformContext {

  /**
   * Create a new dataset instance.
   * @param datasetName the name of the new dataset
   * @param typeName the type of the dataset to create
   * @param properties the properties for the new dataset
   * @throws InstanceConflictException if the dataset already exists
   * @throws DatasetManagementException for any issues encountered in the dataset system,
   *         or if the dataset type's create method fails.
   */
  void createDataset(String datasetName, String typeName, DatasetProperties properties)
    throws DatasetManagementException;

  /**
   * Check whether a dataset exists in the current namespace.
   * @param datasetName the name of the dataset
   * @return whether a dataset of that name exists
   * @throws DatasetManagementException for any issues encountered in the dataset system
   */
  boolean datasetExists(String datasetName) throws DatasetManagementException;

  /**
   * Returns the logical start time of the Batch Job.  Logical start time is the time when this Batch
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Returns runtime arguments of the Batch Job.
   *
   * @return runtime arguments of the Batch Job.
   */
  Map<String, String> getRuntimeArguments();

  /**
   * Updates an entry in the runtime arguments.
   *
   * @param key key to update
   * @param value value to update to
   * @param overwrite if {@code true} and if the key exists in the runtime arguments, it will get overwritten to
   *                  the given value; if {@code false}, the existing value of the key won't get updated.
   */
  void setRuntimeArgument(String key, String value, boolean overwrite);

  /**
   * Returns the hadoop job.
   * @deprecated this method will be removed.
   */
  @Deprecated
  <T> T getHadoopJob();
}
