package com.continuuity.test;

/**
 * Managing the running Service in an application.
 */
public interface ServiceManager {
  /**
   * Stops the running service.
   */
  void stop();

  /**
   * Checks if Service is Running
   */
  boolean isRunning();
}
