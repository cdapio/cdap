package com.continuuity.logging.run;

import com.continuuity.common.twill.InMemoryReactorServiceManager;

/**
 * InMemory LogSaver Service Management class.
 */
public class InMemoryLogSaverServiceMananger extends InMemoryReactorServiceManager {

  @Override
  public boolean isLogAvailable() {
    return false;
  }
}
