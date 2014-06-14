package com.continuuity.logging.run;

import com.continuuity.common.twill.InMemoryReactorServiceManager;

/**
 *
 */
public class InMemoryLogSaverServiceMananger extends InMemoryReactorServiceManager {

  @Override
  public boolean canCheckStatus() {
    return false;
  }

  @Override
  public boolean isLogAvailable() {
    return false;
  }
}
