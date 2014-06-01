package com.continuuity.common.service;

import com.continuuity.common.conf.CConfiguration;

/**
 * Provides interface for starting and stopping a server component.
 */
public interface Server {

  /**
   * Starts the {@link Server}.
   * @param args arguments for the service
   * @param conf instance of configuration object.
   *
   * @throws ServerException If there is an problem when starting the server.
   */
  public void start(String[] args, CConfiguration conf) throws ServerException;

  /**
   * Stops the {@link Server}.
   * @param now true specifies non-graceful shutdown; false otherwise.
   *
   * @throws ServerException If there is an problem when stopping the server.
   */
  public void stop(boolean now) throws ServerException;

}
