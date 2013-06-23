package com.continuuity.common.service;

/**
 * Raised when there is issue in registering, starting, stopping the service.
 */
public class ServerException extends Exception {
  public ServerException(String reason) {
    super(reason);
  }

  public ServerException(String reason, Throwable throwable) {
    super(reason, throwable);
  }

  public ServerException(Throwable throwable) {
    super(throwable);
  }
}
