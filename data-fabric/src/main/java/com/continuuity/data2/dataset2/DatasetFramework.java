package com.continuuity.data2.dataset2;

import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Provides access to the Datasets System.
 *
 * Typical usage example:
 * <tt>
 *   DatasetFramework datasetFramework = ...;
 *   datasetFramework.register("myDatasets", MyDatasetModule.class);
 *   datasetFramework.addInstance("myTable", "table", DatasetInstanceProperties.EMPTY);
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

  // Due to a bug in checkstyle, it would emit false positives here of the form
  // "Unable to get class information for @throws tag '<exn>' (...)".
  // This comment disables that check up to the corresponding ON comments below

  // CHECKSTYLE OFF: @throws

  /**
   * Registers dataset types by adding dataset module to the system.
   * @param moduleName dataset module name
   * @param moduleClass dataset module class
   * @throws ModuleConflictException when module with same name is already registered
   * @throws DatasetManagementException in case of problems
   */
  void register(String moduleName, Class<? extends DatasetModule> moduleClass)
    throws DatasetManagementException;

  /**
   * Deletes dataset module and its types from the system.
   * @param moduleName dataset module name
   * @throws ModuleConflictException when module cannot be deleted because of its dependants
   * @throws DatasetManagementException
   */
  void deleteModule(String moduleName)
    throws DatasetManagementException;

  /**
   * Adds information about dataset instance to the system.
   *
   * This uses
   * {@link com.continuuity.internal.data.dataset.DatasetDefinition#configure(String, DatasetInstanceProperties)}
   * method to build {@link com.continuuity.internal.data.dataset.DatasetInstanceSpec} which describes dataset instance
   * and later used to initialize {@link DatasetAdmin} and {@link Dataset} for the dataset instance.
   *
   * NOTE: It does NOT create physical dataset automatically, use {@link #getAdmin(String, ClassLoader)} to obtain
   *       dataset admin to perform such administrative operations
   * @param datasetTypeName dataset instance type name
   * @param datasetInstanceName dataset instance name
   * @param props dataset instance properties
   * @throws InstanceConflictException if dataset instance with this name already exists
   * @throws DatasetManagementException
   */
  void addInstance(String datasetTypeName, String datasetInstanceName, DatasetInstanceProperties props)
    throws DatasetManagementException;

  /**
   * Deletes dataset instance from the system.
   *
   * NOTE: It will NOT delete physical data set, use {@link #getAdmin(String, ClassLoader)} to obtain
   *       dataset admin to perform such administrative operations
   * @param datasetInstanceName dataset instance name
   * @throws InstanceConflictException if dataset instance cannot be deleted because of its dependencies
   * @throws DatasetManagementException
   */
  void deleteInstance(String datasetInstanceName) throws DatasetManagementException;

  // CHECKSTYLE ON

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
   * @throws IOException when there's trouble to instantiate {@link Dataset}
   */
  @Nullable
  <T extends Dataset> T getDataset(String datasetInstanceName, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException;
}
