package com.continuuity.common.service;

import com.continuuity.common.conf.CConfiguration;

/**
 * Provides interface for starting and stopping the server.
 */
public interface Server {
  /**
   * Starts the {@link Server}
   * @param args arguments for the service
   * @param conf instance of configuration object.
   */
  public void start(String[] args, CConfiguration conf) throws ServerException;

  /**
   * Stops the {@link Server}
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  public void stop(boolean now) throws ServerException;
}
