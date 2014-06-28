package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetModule;

import java.io.IOException;
import java.util.Collection;
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
public interface DatasetFramework {

  /**
   * Adds dataset types by adding dataset module to the system.
   * @param moduleName dataset module name
   * @param module dataset module
   * @throws ModuleConflictException when module with same name is already registered or this module registers a type
   *         with a same name as one of the already registered by another module types
   * @throws DatasetManagementException in case of problems
   */
  void addModule(String moduleName, DatasetModule module)
    throws DatasetManagementException;

  /**
   * Deletes dataset module and its types from the system.
   * @param moduleName dataset module name
   * @throws ModuleConflictException when module cannot be deleted because of its dependant modules or instances
   * @throws DatasetManagementException
   */
  void deleteModule(String moduleName)
    throws DatasetManagementException;

  /**
   * Adds information about dataset instance to the system.
   *
   * This uses
   * {@link com.continuuity.api.dataset.DatasetDefinition#configure(String, DatasetProperties)}
   * method to build {@link com.continuuity.api.dataset.DatasetSpecification} which describes dataset instance
   * and later used to initialize {@link DatasetAdmin} and {@link Dataset} for the dataset instance.
   *
   * NOTE: It does NOT create physical dataset automatically, use {@link #getAdmin(String, ClassLoader)} to obtain
   *       dataset admin to perform such administrative operations
   * @param datasetTypeName dataset instance type name
   * @param datasetInstanceName dataset instance name
   * @param props dataset instance properties
   * @throws InstanceConflictException if dataset instance with this name already exists
   * @throws IOException when creation of dataset instance using its admin fails
   * @throws DatasetManagementException
   */
  void addInstance(String datasetTypeName, String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException, IOException;

  /**
   * @return names of all dataset instances
   * @throws DatasetManagementException
   */
  Collection<String> getInstances() throws DatasetManagementException;

  /**
   * @return true if instance exists, false otherwise
   * @throws DatasetManagementException
   */
  boolean hasInstance(String instanceName) throws DatasetManagementException;

  /**
   * @return true if type exists, false otherwise
   * @throws DatasetManagementException
   */
  boolean hasType(String typeName) throws DatasetManagementException;

  /**
   * Deletes dataset instance from the system.
   *
   * NOTE: It will NOT delete physical data set, use {@link #getAdmin(String, ClassLoader)} to obtain
   *       dataset admin to perform such administrative operations
   * @param datasetInstanceName dataset instance name
   * @throws InstanceConflictException if dataset instance cannot be deleted because of its dependencies
   * @throws IOException when deletion of dataset instance using its admin fails
   * @throws DatasetManagementException
   */
  void deleteInstance(String datasetInstanceName) throws DatasetManagementException, IOException;

  /**
   * Gets dataset instance admin to be used to perform administrative operations.
   * @param datasetInstanceName dataset instance name
   * @param <T> dataset admin type
   * @param classLoader classLoader to be used to load classes or {@code null} to use system classLoader
   * @return instance of dataset admin or {@code null} if dataset instance of this name doesn't exist.
   * @throws DatasetManagementException when there's trouble getting dataset meta info
   * @throws IOException when there's trouble to instantiate {@link DatasetAdmin}
   */
  @Nullable
  <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException;

  /**
   * Gets dataset to be used to perform data operations.
   * @param datasetInstanceName dataset instance name
   * @param <T> dataset type to be returned
   * @param classLoader classLoader to be used to load classes or {@code null} to use system classLoader
   * @return instance of dataset or {@code null} if dataset instance of this name doesn't exist.
   * @throws DatasetManagementException when there's trouble getting dataset meta info
   * @throws IOException when there's trouble to instantiate {@link com.continuuity.api.dataset.Dataset}
   */
  @Nullable
  <T extends Dataset> T getDataset(String datasetInstanceName, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException;
}
