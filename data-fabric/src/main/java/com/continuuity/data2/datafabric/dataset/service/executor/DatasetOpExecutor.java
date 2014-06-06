package com.continuuity.data2.datafabric.dataset.service.executor;

import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.google.common.util.concurrent.Service;

import java.io.IOException;

/**
 * Executes various Dataset operations.
 */
public interface DatasetOpExecutor extends Service {

  /**
   * @param instanceName Name of the dataset instance.
   * @return true if dataset exists
   * @throws IOException
   */
  boolean exists(String instanceName) throws Exception;

  /**
   * Creates dataset.
   * @param instanceName Name of the dataset instance.
   * @param typeMeta Data set type meta
   * @param props Data set instance properties
   * @throws IOException
   */
  DatasetInstanceSpec create(String instanceName, DatasetTypeMeta typeMeta, DatasetInstanceProperties props)
    throws Exception;

  /**
   * Drops dataset.
   * @param typeMeta Data set type meta
   * @param spec Data set instance spec
   * @throws IOException
   */
  void drop(DatasetInstanceSpec spec, DatasetTypeMeta typeMeta) throws Exception;

  /**
   * Deletes all data of the dataset.
   * @param instanceName Name of the dataset instance.
   * @throws IOException
   */
  void truncate(String instanceName) throws Exception;

  /**
   * Upgrades dataset.
   * @param instanceName Name of the dataset instance.
   * @throws IOException
   */
  void upgrade(String instanceName) throws Exception;

}
