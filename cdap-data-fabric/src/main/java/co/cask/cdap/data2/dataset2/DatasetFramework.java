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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides access to the Datasets System.
 *
 * Typical usage example:
 * <tt>
 *   DatasetFramework datasetFramework = ...;
 *   datasetFramework.addModule("myDatasets", MyDatasetModule.class);
 *   datasetFramework.addInstance("myTable", "table", DatasetProperties.EMPTY);
 *   TableAdmin admin = datasetFramework.getAdmin("myTable");
 *   admin.create();
 *   Table table = datasetFramework.getDataset("myTable");
 *   try {
 *     table.write("key", "value");
 *   } finally {
 *     table.close();
 *   }
 * </tt>
 */
// todo: use dataset instead of dataset instance in namings
public interface DatasetFramework {

  /**
   * Adds dataset types by adding dataset module to the system.
   *
   * @param moduleId dataset module id
   * @param module dataset module
   * @throws ModuleConflictException when module with same name is already registered or this module registers a type
   *         with a same name as one of the already registered by another module types
   * @throws DatasetManagementException in case of problems
   */
  void addModule(Id.DatasetModule moduleId, DatasetModule module) throws DatasetManagementException;

  /**
   * Deletes dataset module and its types from the system.
   *
   * @param moduleId dataset module id
   * @throws ModuleConflictException when module cannot be deleted because of its dependant modules or instances
   * @throws DatasetManagementException
   */
  void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException;

  /**
   * Deletes dataset modules and its types in the specified namespace.
   *
   * @param namespaceId the {@link Id.Namespace} to delete all modules from.
   * @throws ModuleConflictException when some of modules can't be deleted because of its dependant modules or instances
   * @throws DatasetManagementException
   */
  void deleteAllModules(Id.Namespace namespaceId) throws DatasetManagementException;

  /**
   * Adds information about dataset instance to the system.
   *
   * This uses
   * {@link co.cask.cdap.api.dataset.DatasetDefinition#configure(String, DatasetProperties)}
   * method to build {@link co.cask.cdap.api.dataset.DatasetSpecification} which describes dataset instance
   * and later used to initialize {@link DatasetAdmin} and {@link Dataset} for the dataset instance.
   *
   * @param datasetTypeName dataset instance type name
   * @param datasetInstanceId dataset instance name
   * @param props dataset instance properties
   * @throws InstanceConflictException if dataset instance with this name already exists
   * @throws IOException when creation of dataset instance using its admin fails
   * @throws DatasetManagementException
   */
  void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException;

  /**
   * Updates the existing dataset instance in the system.
   *
   * This uses
   * {@link co.cask.cdap.api.dataset.DatasetDefinition#configure(String, DatasetProperties)}
   * method to build {@link co.cask.cdap.api.dataset.DatasetSpecification} with new properties,
   * which describes dataset instance and {@link DatasetAdmin} is used to upgrade
   * {@link Dataset} for the dataset instance.
   * @param datasetInstanceId dataset instance name
   * @param props dataset instance properties
   * @throws IOException when creation of dataset instance using its admin fails
   * @throws DatasetManagementException
   */
  void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException;

  /**
   * Get all dataset instances in the specified namespace
   *
   * @param namespaceId the specified namespace id
   * @return a collection of {@link DatasetSpecification}s for all datasets in the specified namespace
   */
  Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId) throws DatasetManagementException;

  /**
   * Gets the {@link DatasetSpecification} for the specified dataset instance id
   *
   * @param datasetInstanceId the {@link Id.DatasetInstance} for which the {@link DatasetSpecification} is desired
   * @return {@link DatasetSpecification} of the dataset or {@code null} if dataset not not exist
   */
  @Nullable
  DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException;

  /**
   * @param datasetInstanceId the {@link Id.DatasetInstance} to check for existence
   * @return true if instance exists, false otherwise
   * @throws DatasetManagementException
   */
  boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException;

  /**
   * Checks if the specified type exists in the 'system' namespace
   *
   * @return true if type exists in the 'system' namespace, false otherwise
   * @throws DatasetManagementException
   */
  boolean hasSystemType(String typeName) throws DatasetManagementException;

  /**
   * Checks if the specified type exists in the specified namespace
   *
   * @return true if type exists in the specified namespace, false otherwise
   * @throws DatasetManagementException
   */
  @VisibleForTesting
  boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException;

  /**
   * Deletes dataset instance from the system.
   *
   * @param datasetInstanceId dataset instance name
   * @throws InstanceConflictException if dataset instance cannot be deleted because of its dependencies
   * @throws IOException when deletion of dataset instance using its admin fails
   * @throws DatasetManagementException
   */
  void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException;

  /**
   * Deletes all dataset instances in the specified namespace.
   *
   * @param namespaceId the specified namespace id
   * @throws IOException when deletion of dataset instance using its admin fails
   * @throws DatasetManagementException
   */
  void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException;

  /**
   * Gets dataset instance admin to be used to perform administrative operations.
   *
   * @param <T> dataset admin type
   * @param datasetInstanceId dataset instance name
   * @param classLoader classLoader to be used to load classes or {@code null} to use system classLoader
   * @return instance of dataset admin or {@code null} if dataset instance of this name doesn't exist.
   * @throws DatasetManagementException when there's trouble getting dataset meta info
   * @throws IOException when there's trouble to instantiate {@link DatasetAdmin}
   */
  @Nullable
  <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException;

  /**
   * Gets dataset to be used to perform data operations.
   *
   * @param <T> dataset type to be returned
   * @param datasetInstanceId dataset instance id
   * @param arguments runtime arguments for the dataset instance
   * @param classLoader classLoader to be used to load classes or {@code null} to use system classLoader
   * @param owners owners of the dataset
   * @return instance of dataset or {@code null} if dataset instance of this name doesn't exist.
   * @throws DatasetManagementException when there's trouble getting dataset meta info
   * @throws IOException when there's trouble to instantiate {@link co.cask.cdap.api.dataset.Dataset}
   */
  @Nullable
  <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                   @Nullable ClassLoader classLoader, @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException;

  /**
   * Gets dataset to be used to perform data operations.
   *
   * @param <T> dataset type to be returned
   * @param datasetInstanceId dataset instance id
   * @param arguments runtime arguments for the dataset instance
   * @param classLoader classLoader to be used to load classes or {@code null} to use system classLoader
   * @return instance of dataset or {@code null} if dataset instance of this name doesn't exist.
   * @throws DatasetManagementException when there's trouble getting dataset meta info
   * @throws IOException when there's trouble to instantiate {@link co.cask.cdap.api.dataset.Dataset}
   */
  @Nullable
  <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                   @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException;

  /**
   * Gets dataset to be used to perform data operations.
   *
   * @param <T> dataset type to be returned
   * @param datasetInstanceId dataset instance id
   * @param arguments runtime arguments for the dataset instance
   * @param classLoaderProvider providers to get classloaders for different dataset modules
   * @param owners owners of the dataset
   * @return instance of dataset or {@code null} if dataset instance of this name doesn't exist.
   * @throws DatasetManagementException when there's trouble getting dataset meta info
   * @throws IOException when there's trouble to instantiate {@link co.cask.cdap.api.dataset.Dataset}
   */
  @Nullable
  <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                   DatasetClassLoaderProvider classLoaderProvider,
                                   @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException;

  void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException;

  void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException;
}
