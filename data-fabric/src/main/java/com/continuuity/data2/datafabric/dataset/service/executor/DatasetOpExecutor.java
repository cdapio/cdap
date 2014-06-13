package com.continuuity.data2.datafabric.dataset.service.executor;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
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
  DatasetSpecification create(String instanceName, DatasetTypeMeta typeMeta, DatasetProperties props)
    throws Exception;

  /**
   * Drops dataset.
   * @param typeMeta Data set type meta
   * @param spec Data set instance spec
   * @throws IOException
   */
  void drop(DatasetSpecification spec, DatasetTypeMeta typeMeta) throws Exception;

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
