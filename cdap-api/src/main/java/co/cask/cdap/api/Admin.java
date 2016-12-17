/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.messaging.MessagingAdmin;
import co.cask.cdap.api.security.store.SecureStoreManager;

/**
 * This interface provides methods for operational calls from within a CDAP application.
 */
@Beta
public interface Admin extends SecureStoreManager, MessagingAdmin {
  /**
   * Check whether a dataset exists in the current namespace.
   * @param name the name of the dataset
   * @return whether a dataset of that name exists
   * @throws DatasetManagementException for any issues encountered in the dataset system
   */
  boolean datasetExists(String name) throws DatasetManagementException;

  /**
   * Get the type of a dataset.
   * @param name the name of a dataset
   * @return the type of the dataset, if it exists; null otherwise
   * @throws InstanceNotFoundException if the dataset does not exist
   * @throws DatasetManagementException for any issues encountered in the dataset system
   */
  String getDatasetType(String name) throws DatasetManagementException;

  /**
   * Get the properties with which a dataset was created or updated.
   * @param name the name of the dataset
   * @return The properties that were used to create or update the dataset, or null if the dataset does not exist.
   * @throws InstanceNotFoundException if the dataset does not exist
   * @throws DatasetManagementException for any issues encountered in the dataset system
   */
  DatasetProperties getDatasetProperties(String name) throws DatasetManagementException;

  /**
   * Create a new dataset instance.
   * @param name the name of the new dataset
   * @param type the type of the dataset to create
   * @param properties the properties for the new dataset
   * @throws InstanceConflictException if the dataset already exists
   * @throws DatasetManagementException for any issues encountered in the dataset system,
   *         or if the dataset type's create method fails.
   */
  void createDataset(String name, String type, DatasetProperties properties) throws DatasetManagementException;

  /**
   * Update an existing dataset with new properties.
   * @param name the name of the dataset
   * @param properties the new properties for the dataset
   * @throws InstanceNotFoundException if the dataset does not exist
   * @throws DatasetManagementException for any issues encountered in the dataset system,
   *         or if the dataset type's update method fails.
   */
  void updateDataset(String name, DatasetProperties properties) throws DatasetManagementException;

  /**
   * Delete a dataset instance.
   * @param name the name of the dataset
   * @throws InstanceNotFoundException if the dataset does not exist
   * @throws DatasetManagementException for any issues encountered in the dataset system,
   *         or if the dataset type's drop method fails.
   */
  void dropDataset(String name) throws DatasetManagementException;

  /**
   * Truncate a dataset, that is, delete all its data.
   * @param name the name of the dataset
   * @throws InstanceNotFoundException if the dataset does not exist
   * @throws DatasetManagementException for any issues encountered in the dataset system,
   *         or if the dataset type's truncate method fails.
   */
  void truncateDataset(String name) throws DatasetManagementException;
}
