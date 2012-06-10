package com.continuuity.common.service;

/**
 * Raised when there is issue in registering, starting, stoping the service.
 */
public class ServerException extends Exception {
  public ServerException(String reason) {
    super(reason);
  }
}
