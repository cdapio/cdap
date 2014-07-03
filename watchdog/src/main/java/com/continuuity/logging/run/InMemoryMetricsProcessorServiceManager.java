package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractInMemoryReactorServiceManager;

/**
 * InMemory MetricsProcessor Service Management class.
 */
public class InMemoryMetricsProcessorServiceManager extends AbstractInMemoryReactorServiceManager {

  @Override
  public String getDescription() {
    return Constants.MetricsProcessor.SERVICE_DESCRIPTION;
  }

  @Override
  public boolean isLogAvailable() {
    return false;
  }
}
