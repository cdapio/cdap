package com.continuuity.test;

import com.continuuity.app.Id;

/**
 * Managing the running Service in an application.
 */
public interface ServiceManager {
  /**
   * Stops the running service.
   */
  void stop();

  /**
   * Status of Service
   */
  ProgramStatus getStatus();
}
