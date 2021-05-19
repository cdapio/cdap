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

package io.cdap.cdap.etl.api.batch;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.action.SettableArguments;

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
    throws DatasetManagementException, AccessException;

  /**
   * Check whether a dataset exists in the current namespace.
   * @param datasetName the name of the dataset
   * @return whether a dataset of that name exists
   * @throws DatasetManagementException for any issues encountered in the dataset system
   */
  boolean datasetExists(String datasetName) throws DatasetManagementException, AccessException;

  /**
   * Returns settable pipeline arguments. These arguments are shared by all pipeline stages, so plugins should be
   * careful to prefix any arguments that should not be clobbered by other pipeline stages.
   *
   * @return settable pipeline arguments
   */
  @Override
  SettableArguments getArguments();
}
