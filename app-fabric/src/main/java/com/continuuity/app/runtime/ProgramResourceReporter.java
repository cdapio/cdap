package com.continuuity.app.runtime;

/**
 * Reports resource usage of a program.
 */
public interface ProgramResourceReporter {

  /**
   * Start the reporter
   */
  public void start();

  /**
   * Stop the reporter
   */
  public void stop();

  /**
   * Report resource usage of a program.  Implementors will likely want to write usage to persistant storage.
   */
  public void reportResources();
}
