package com.continuuity.data2.dataset2.manager;

import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Provides access to the Datasets System.
 *
 * Typical usage example:
 * <tt>
 *   DatasetManager datasetManager = ...;
 *   datasetManager.register("myDatasets", MyDatasetModule.class);
 *   datasetManager.addInstance("myTable", "table", DatasetInstanceProperties.EMPTY);
 *   TableAdmin admin = datasetManager.getAdmin("myTable");
 *   admin.create();
 *   Table table = datasetManager.getDataset("myTable");
 *   try {
 *     table.write("key", "value");
 *   } finally {
 *     table.close();
 *   }
 * </tt>
 */
public interface DatasetManager {
  /**
   * Registers dataset types by adding dataset module to the system.
   * @param moduleName dataset module name
   * @param moduleClass dataset module class
   * @throws ModuleConflictException when module with same name is already registered
   * @throws IOException
   */
  void register(String moduleName, Class<? extends DatasetModule> moduleClass)
    throws ModuleConflictException, IOException;

  /**
   * Deletes dataset module and its types from the system.
   * @param moduleName dataset module name
   * @throws ModuleConflictException when module cannot be deleted because of its dependants
   * @throws IOException
   */
  void deleteModule(String moduleName)
    throws ModuleConflictException, IOException;

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
   * @throws IOException
   */
  void addInstance(String datasetTypeName, String datasetInstanceName, DatasetInstanceProperties props)
    throws InstanceConflictException, IOException;

  /**
   * Deletes dataset instance from the system.
   *
   * NOTE: It will NOT delete physical data set, use {@link #getAdmin(String, ClassLoader)} to obtain
   *       dataset admin to perform such administrative operations
   * @param datasetInstanceName dataset instance name
   * @throws InstanceConflictException if dataset instance cannot be deleted because of its dependencies
   * @throws IOException
   */
  void deleteInstance(String datasetInstanceName) throws InstanceConflictException, IOException;

  /**
   * Gets dataset instance admin to be used to perform administrative operations.
   * @param datasetInstanceName dataset instance name
   * @param <T> dataset admin type
   * @param classLoader classLoader to be used to load classes or {@code null} to use system classLoader
   * @return instance of dataset admin or {@code null} if dataset instance of this name doesn't exist.
   * @throws IOException
   */
  @Nullable
  <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, @Nullable ClassLoader classLoader)
    throws IOException;

  /**
   * Gets dataset to be used to perform data operations.
   * @param datasetInstanceName dataset instance name
   * @param <T> dataset type to be returned
   * @param classLoader classLoader to be used to load classes or {@code null} to use system classLoader
   * @return instance of dataset or {@code null} if dataset instance of this name doesn't exist.
   * @throws IOException
   */
  @Nullable
  <T extends Dataset> T getDataset(String datasetInstanceName, @Nullable ClassLoader classLoader)
    throws IOException;
}
