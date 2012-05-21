package com.continuuity.common.service;

import org.apache.hadoop.conf.Configuration;

/**
 * RegisteredService provides ability to registered service
 */
public interface RegisteredService {
  /**
   * Starts the {@link RegisteredService}
   * @param args arguments for the service
   * @param conf instance of configuration object.
   */
  public void start(String[] args, Configuration conf) throws RegisteredServiceException;

  /**
   * Stops the service
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  public void stop(boolean now) throws RegisteredServiceException;
}
