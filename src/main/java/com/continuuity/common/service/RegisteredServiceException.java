package com.continuuity.common.service;

/**
 * Raised when there is issue in registering, starting, stoping the service.
 */
public class RegisteredServiceException extends Exception {
  public RegisteredServiceException(String reason) {
    super(reason);
  }
}
