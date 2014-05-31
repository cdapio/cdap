package com.continuuity.data2.datafabric.dataset.service.executor;

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
   * @throws IOException
   */
  void create(String instanceName) throws Exception;

  /**
   * Drops dataset.
   * @param instanceName Name of the dataset instance.
   * @throws IOException
   */
  void drop(String instanceName) throws Exception;

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
