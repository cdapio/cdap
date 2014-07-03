package com.continuuity.logging.run;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractInMemoryReactorServiceManager;

/**
 *  In memory metrics service manager.
 */
public class InMemoryMetricsServiceManager extends AbstractInMemoryReactorServiceManager {

  @Override
  public String getDescription() {
    return Constants.Metrics.SERVICE_DESCRIPTION;
  }
}
