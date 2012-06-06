package com.continuuity.common.service;

import com.continuuity.common.conf.CConfiguration;

/**
 * RegisteredService provides ability to registered service
 */
public interface RegisteredService {
  /**
   * Starts the {@link RegisteredService}
   * @param args arguments for the service
   * @param conf instance of configuration object.
   */
  public void start(String[] args, CConfiguration conf) throws RegisteredServiceException;

  /**
   * Stops the service
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  public void stop(boolean now) throws RegisteredServiceException;
}
