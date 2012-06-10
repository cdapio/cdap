package com.continuuity.common.service;

import com.continuuity.common.conf.CConfiguration;

/**
 * RegisteredService provides the ability to wrap a standalone component and
 * register it as a service in our central service directory.
 */
public interface RegisteredService {

  /**
   * Starts the {@link RegisteredService}
   * @param args arguments for the service
   * @param conf instance of configuration object.
   *
   * @throws ServerException If there is any problem starting.
   */
  public void start(String[] args, CConfiguration conf)
    throws ServerException;

  /**
   * Stops the service
   * @param now true specifies non-graceful shutdown; false otherwise.
   *
   * @throws ServerException If there is a problem stopping.
   */
  public void stop(boolean now) throws ServerException;

}
