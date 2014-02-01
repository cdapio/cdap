package com.continuuity.data2.dataset2.manager;

import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import javax.annotation.Nullable;

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
   * @throws Exception
   */
  void register(String moduleName, Class<? extends DatasetModule> moduleClass) throws Exception;

  /**
   * Adds information about dataset instance to the system.
   *
   * This uses
   * {@link com.continuuity.internal.data.dataset.DatasetDefinition#configure(String, DatasetInstanceProperties)}
   * method to build {@link com.continuuity.internal.data.dataset.DatasetInstanceSpec} which describes dataset instance
   * and later used to initialize {@link DatasetAdmin} and {@link Dataset} for the dataset instance.
   *
   * NOTE: It does NOT create dataset instance automatically, use {@link #getAdmin(String)} to obtain dataset admin to
   *       perform such administrative operations
   * @param datasetTypeName dataset instance type name
   * @param datasetInstanceName dataset instance name
   * @param props dataset instance properties
   * @throws IllegalArgumentException if dataset instance with this name already exists
   * @throws Exception
   */
  void addInstance(String datasetTypeName, String datasetInstanceName, DatasetInstanceProperties props)
    throws Exception;

  /**
   * Gets dataset instance admin to be used to perform administrative operations.
   * @param datasetInstanceName dataset instance name
   * @param <T> dataset admin type
   * @return instance of dataset admin or {@code null} if dataset instance of this name doesn't exist.
   * @throws Exception
   */
  @Nullable
  <T extends DatasetAdmin> T getAdmin(String datasetInstanceName) throws Exception;

  /**
   * Gets dataset to be used to perform data operations.
   * @param datasetInstanceName dataset instance name
   * @param <T> dataset type to be returned
   * @return instance of dataset or {@code null} if dataset instance of this name doesn't exist.
   * @throws Exception
   */
  @Nullable
  <T extends Dataset> T getDataset(String datasetInstanceName) throws Exception;
}
