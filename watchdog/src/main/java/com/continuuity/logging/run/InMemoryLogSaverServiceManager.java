package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractInMemoryReactorServiceManager;

/**
 * InMemory LogSaver Service Management class.
 */
public class InMemoryLogSaverServiceManager extends AbstractInMemoryReactorServiceManager {

  @Override
  public boolean isLogAvailable() {
    return false;
  }

  @Override
  public String getDescription() {
    return Constants.LogSaver.SERVICE_DESCRIPTION;
  }
}
