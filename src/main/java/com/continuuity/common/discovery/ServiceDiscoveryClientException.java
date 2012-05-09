package com.continuuity.common.discovery;

/**
 * An exception that is thrown when a service registration fails.
 */
public class ServiceDiscoveryClientException extends Exception {

  /**
   * Constructs a ServiceDiscoveryClientException with null as it's detailed message.
   */
  public ServiceDiscoveryClientException() {
    super();
  }

  /**
   * Constructs a new ServiceDiscoveryClientException with the specified detail message.
   * @param message the detail message. The details is saved for later retrieval by the {@link Throwable#getMessage()}
   *                method.
   */
  public ServiceDiscoveryClientException(String message) {
    super(message);
  }

  /**
   * Constructs a new ServiceDiscoveryClientException with the specified detail message and cause.
   * @param message the detail message. The details is saved for later retrieval by the {@link Throwable#getMessage()}
   *                method.
   * @param cause the cause
   */
  public ServiceDiscoveryClientException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new ServiceDiscoveryClientException with the specified cause and detail message of cause. This
   * type of constructor are useful for exceptions that are little more than wrappers for other throwables.
   * @param cause the cause.
   */
  public ServiceDiscoveryClientException(Throwable cause) {
    super(cause);
  }
}
