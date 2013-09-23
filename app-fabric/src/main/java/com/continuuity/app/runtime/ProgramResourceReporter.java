package com.continuuity.app.runtime;


import com.google.common.util.concurrent.Service;

/**
 * Reports resource usage of a program.
 */
public interface ProgramResourceReporter extends Service {
  /**
   * Report resource usage of a program.  Implementors will likely want to write usage to persistant storage.
   */
  public void reportResources();
}
